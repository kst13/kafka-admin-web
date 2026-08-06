package com.osstem.kafkaadmin.auth;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

@Component
public class AdminUserSeeder implements CommandLineRunner {

    private final UserRepository users;
    private final PasswordEncoder encoder;
    private final String initialPassword;

    public AdminUserSeeder(UserRepository users, PasswordEncoder encoder,
                           @Value("${app.admin-initial-password}") String initialPassword) {
        this.users = users;
        this.encoder = encoder;
        this.initialPassword = initialPassword;
    }

    @Override
    public void run(String... args) {
        if (users.count() == 0 && !initialPassword.isBlank()) {
            users.save(new AppUser("admin", encoder.encode(initialPassword), "ADMIN"));
        }
    }
}
