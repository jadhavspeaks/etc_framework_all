package com.example.etl.api.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import static org.springframework.security.config.Customizer.withDefaults;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true) // Enable method-level security like @PreAuthorize
public class SecurityConfig {

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public UserDetailsService userDetailsService(PasswordEncoder passwordEncoder) {
        UserDetails adminUser = User.builder()
            .username("admin")
            .password(passwordEncoder.encode("adminpass"))
            .roles("ADMIN", "EDITOR", "VIEWER")
            .build();

        UserDetails editorUser = User.builder()
            .username("editor")
            .password(passwordEncoder.encode("editorpass"))
            .roles("EDITOR", "VIEWER")
            .build();

        UserDetails viewerUser = User.builder()
            .username("viewer")
            .password(passwordEncoder.encode("viewerpass"))
            .roles("VIEWER")
            .build();

        return new InMemoryUserDetailsManager(adminUser, editorUser, viewerUser);
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .csrf().disable() // Disable CSRF for stateless API, or configure properly if needed
            .authorizeRequests(authorize -> authorize
                // .antMatchers("/public/**").permitAll() // Example for public endpoints
                .antMatchers("/api/v1/jobconfigs/**").authenticated() // Secure all jobconfigs endpoints
                .anyRequest().authenticated() // Secure any other unlisted request
            )
            .httpBasic(withDefaults()); // Enable HTTP Basic Authentication
        return http.build();
    }
}
