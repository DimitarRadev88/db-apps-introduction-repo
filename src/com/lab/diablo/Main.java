package com.lab.diablo;

import java.sql.*;
import java.util.Properties;
import java.util.Scanner;

public class Main {

    public static final String ROOT = "root";
    public static final String PASSWORD = "peanutbutter";
    public static final String MYSQL_CONNECTION = "jdbc:mysql://localhost:3306/diablo";

    public static void main(String[] args) throws SQLException {
        Scanner scanner = new Scanner(System.in);

        Properties properties = new Properties();

        properties.setProperty("user", ROOT);
        properties.setProperty("password", PASSWORD);

        Connection connection = DriverManager.getConnection(MYSQL_CONNECTION, properties);

        PreparedStatement preparedStatement = connection.prepareStatement("""
                SELECT user_name, first_name, last_name, COUNT(game_id) AS 'games_count'
                From users
                JOIN users_games ug
                ON users.id = ug.user_id
                WHERE user_name = ?
                GROUP BY user_id
                """);

        System.out.print("Enter username: ");
        preparedStatement.setString(1, scanner.nextLine());

        ResultSet rs = preparedStatement.executeQuery();

        if (!rs.next()) {
            System.out.println("No such user exists");
            return;
        }

        String userName = rs.getString("user_name");
        String firstName = rs.getString("last_name");
        String lastName = rs.getString("first_name");
        int gamesCount = rs.getInt("games_count");

        System.out.printf("""
                User: %s
                %s %s has played %d games
                """,
                userName,
                firstName, lastName, gamesCount);
    }

}
