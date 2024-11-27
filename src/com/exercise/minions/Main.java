package com.exercise.minions;

import java.sql.*;
import java.util.*;

public class Main {
    private static final Scanner SCANNER = new Scanner(System.in);
    public static Connection CONNECTION;
    private static final String DEFAULT_EVILNESS_FACTOR = "evil";

    public static void main(String[] args) throws SQLException {
        CONNECTION = getConnection();

//        getVillainsNames();
//        getMinionNames();
//        addMinionToDb();
//        changeTownNamesCasing();
//        removeVillain();
//        printAllMinionNames();
//        increaseMinionsAge();
//        increaseAgeStoredProcedure();
    }

    private static void increaseAgeStoredProcedure() throws SQLException {
        long minionId = Long.parseLong(SCANNER.nextLine());
        callGetOlderProcedure(minionId);

        printMinionNameAndAge(minionId);

    }

    private static void printMinionNameAndAge(long minionId) throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("""
                SELECT name, age
                FROM minions
                WHERE id = ?
                """);

        ps.setLong(1, minionId);
        ResultSet rs = ps.executeQuery();

        rs.next();

        System.out.println(rs.getString("name") + " " + rs.getInt("age"));
    }

    private static void callGetOlderProcedure(long minionId) throws SQLException {
        CallableStatement cs = CONNECTION.prepareCall("{call usp_get_older(?)}");

        cs.setLong(1, minionId);

        cs.executeUpdate();
    }

    private static void increaseMinionsAge() throws SQLException {
        long[] ids = Arrays.stream(SCANNER.nextLine().split(" ")).mapToLong(Long::parseLong).toArray();

        updateMinionsNamesAndAgesWithIds(ids);

        printAllMinionsNamesAndAges();

    }

    private static void printAllMinionsNamesAndAges() throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("""
                SELECT name, age
                FROM minions
                """);

        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            String name = rs.getString("name");
            int age = rs.getInt("age");
            System.out.println(name + " " + age);
        }
    }

    private static void updateMinionsNamesAndAgesWithIds(long[] ids) throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("""
                UPDATE minions
                SET name = LOWER(name), age = age + 1
                WHERE id = ?;
                """);

        for (long id : ids) {
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }

    private static void printAllMinionNames() throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("SELECT name FROM minions");
        ResultSet rs = ps.executeQuery();

        List<String> minionNames = new ArrayList<>();

        while (rs.next()) {
            minionNames.add(rs.getString("name"));
        }

        for (int i = 0; i < minionNames.size() / 2; i++) {
            System.out.println(minionNames.get(i));
            System.out.println(minionNames.get(minionNames.size() - i - 1));
        }

    }

    private static void removeVillain() throws SQLException {
        CONNECTION.setAutoCommit(false);

        long villainId = Long.parseLong(SCANNER.nextLine());

        deleteVillainAndReleaseMinions(villainId);

    }

    private static void deleteVillainAndReleaseMinions(long villainId) throws SQLException {
        Optional<Long> minionsReleased = Optional.empty();

        try {
            minionsReleased = Optional.of(releaseMinionsFromVillain(villainId));
            System.out.println(deleteVillain(villainId));
            minionsReleased.ifPresent(aLong -> System.out.println(aLong + " minions released"));
            CONNECTION.commit();
        } catch (SQLException e) {
            CONNECTION.rollback();
        } catch (IllegalArgumentException e) {
            CONNECTION.rollback();
            System.out.println(e.getMessage());
        }
    }

    private static String deleteVillain(long villainId) throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("""
                SELECT name FROM villains
                WHERE id = ?
                """);

        ps.setLong(1, villainId);
        ResultSet rs = ps.executeQuery();
        if (!rs.next()) {
            throw new IllegalArgumentException("No such villain was found");
        }
        String villainName = rs.getString("name");

        ps = CONNECTION.prepareStatement("""
                DELETE FROM villains
                WHERE id = ?;
                """);

        ps.setLong(1, villainId);

        ps.executeUpdate();

        return villainName + " was deleted";
    }

    private static long releaseMinionsFromVillain(long villainId) throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("""
                DELETE FROM minions_villains
                WHERE villain_id = ?;
                """);

        ps.setLong(1, villainId);
        return ps.executeUpdate();
    }

    private static void changeTownNamesCasing() throws SQLException {
        String countryName = SCANNER.nextLine();

        int affectedNames = transformTownNamesToUppercase(countryName);

        if (affectedNames == 0) {
            System.out.println("No town names were affected.");
            return;
        }

        System.out.println(affectedNames + " town names were affected.");
        printCountryNames(countryName, affectedNames);
    }

    private static void printCountryNames(String countryName, int affectedNames) throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("""
                SELECT name FROM towns
                WHERE country = ?
                """);
        ps.setString(1, countryName);

        ResultSet rs = ps.executeQuery();

        String[] townNames = new String[affectedNames];
        int i = 0;
        while (rs.next()) {
            townNames[i++] = rs.getString("name");
        }


        System.out.println("[" + String.join(", ", townNames) + "]");
    }

    private static int transformTownNamesToUppercase(String countryName) throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("""
                UPDATE towns
                SET towns.name = UPPER(towns.name)
                WHERE country = ?;
                """);

        ps.setString(1, countryName);

        return ps.executeUpdate();
    }

    private static void addMinionToDb() throws SQLException {
        String[] minionInfo = SCANNER.nextLine().substring("Minion: ".length()).split(" ");

        String minionName = minionInfo[0];
        int minionAge = Integer.parseInt(minionInfo[1]);
        String townName = minionInfo[2];

        String villainName = SCANNER.nextLine().substring("Villain: ".length());

        Optional<Long> townId;

        try {
            townId = Optional.of(getTownId(townName));
        } catch (IllegalArgumentException e) {
            townId = Optional.of(addTownToDb(townName));
        }

        long minionId = addMinionToDb(minionName, minionAge, townId.get());

        Optional<Long> villainId;

        try {
            villainId = Optional.of(getVillainId(villainName));
        } catch (IllegalArgumentException e) {
            villainId = Optional.of(addVillainToDb(villainName));
        }

        System.out.println(assignMinionToVillain(villainName, minionName, villainId.get(), minionId));
    }

    private static String assignMinionToVillain(String villainName, String minionName, long villainId, long minionId) throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("""
                INSERT INTO minions_villains (minion_id, villain_id)
                VALUES (?, ?);
                """);

        ps.setLong(1, minionId);
        ps.setLong(2, villainId);

        ps.executeUpdate();

        return String.format("Successfully added %s to be minion of %s", minionName, villainName);
    }

    private static long addVillainToDb(String villainName) throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("""
                        INSERT INTO villains (name, evilness_factor)
                        VALUES (?, ?);
                        """,
                Statement.RETURN_GENERATED_KEYS);

        ps.setString(1, villainName);
        ps.setString(2, DEFAULT_EVILNESS_FACTOR);
        ps.executeUpdate();
        ResultSet rs = ps.getGeneratedKeys();
        rs.next();

        System.out.println("Villain " + villainName + " was added to the database.");

        return rs.getLong(1);
    }

    private static long getVillainId(String villainName) throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("""
                SELECT id
                FROM villains v
                WHERE name = ?;
                """);
        ps.setString(1, villainName);

        ResultSet rs = ps.executeQuery();

        if (rs.next()) {
            return rs.getLong("id");
        }

        throw new IllegalArgumentException("Villain does not exist in database!");

    }

    private static long addMinionToDb(String minionName, int minionAge, long townId) throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("""
                        INSERT INTO minions (name, age, town_id)
                        VALUES (?, ?, ?)
                        """,
                Statement.RETURN_GENERATED_KEYS);

        ps.setString(1, minionName);
        ps.setInt(2, minionAge);
        ps.setLong(3, townId);

        ps.executeUpdate();
        ResultSet rs = ps.getGeneratedKeys();
        rs.next();
        return rs.getLong(1);
    }

    private static long addTownToDb(String townName) throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("""
                        INSERT INTO towns(name)
                        VALUE (?)
                        """,
                Statement.RETURN_GENERATED_KEYS);

        ps.setString(1, townName);
        ps.executeUpdate();
        ResultSet rs = ps.getGeneratedKeys();
        rs.next();

        System.out.println("Town " + townName + " was added to the database.");

        return rs.getLong(1);
    }

    private static long getTownId(String townName) throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("SELECT id FROM towns WHERE name = ?");
        ps.setString(1, townName);
        ResultSet rs = ps.executeQuery();

        if (rs.next()) {
            return rs.getInt("id");
        }

        throw new IllegalArgumentException("Town does not exist in database!");
    }

    private static void getMinionNames() throws SQLException {

        PreparedStatement ps = CONNECTION.prepareStatement("""
                SELECT v.name AS villain_name, m.name AS minion_name, m.age AS minion_age
                FROM villains AS v
                JOIN minions_villains m_v
                    ON v.id = m_v.villain_id
                JOIN minions m
                    ON m.id = m_v.minion_id
                WHERE v.id = ?
                """);

        int villainId = Integer.parseInt(SCANNER.nextLine());
        ps.setInt(1, villainId);

        ResultSet rs = ps.executeQuery();

        Map<String, Map<String, Integer>> villainsMinions = new LinkedHashMap<>();
        if (!rs.next()) {
            System.out.println("No villain with ID " + villainId + " exists in the database.");
            return;
        }

        String villainName = rs.getString("villain_name");
        villainsMinions.put(villainName, new LinkedHashMap<>());
        String minionName = rs.getString("minion_name");
        int minionAge = rs.getInt("minion_age");
        villainsMinions.get(villainName).put(minionName, minionAge);

        while (rs.next()) {
            minionName = rs.getString("minion_name");
            minionAge = rs.getInt("minion_age");
            villainsMinions.get(villainName).put(minionName, minionAge);
        }

        System.out.println("Villain: " + villainName);

        int i = 1;
        for (Map.Entry<String, Integer> entry : villainsMinions.get(villainName).entrySet()) {
            System.out.println(i++ + " " + entry.getKey() + " " + entry.getValue());
        }
    }


    private static void getVillainsNames() throws SQLException {
        PreparedStatement ps = CONNECTION.prepareStatement("""
                SELECT name, COUNT(minion_id) AS minions_count
                FROM villains AS v
                JOIN minions_villains m_v
                    ON v.id = m_v.villain_id
                GROUP BY v.id
                HAVING minions_count > 15
                ORDER BY minions_count DESC;
                """);

        ResultSet rs = ps.executeQuery();

        List<String> minionsInfo = new ArrayList<>();

        while (rs.next()) {
            String name = rs.getString("name");
            int minionsCount = rs.getInt("minions_count");
            minionsInfo.add(name + " " + minionsCount);
        }

        minionsInfo.forEach(System.out::println);
    }

    private static Connection getConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "peanutbutter");

        return DriverManager.getConnection("jdbc:mysql://localhost:3306/minions_db", properties);
    }

}
