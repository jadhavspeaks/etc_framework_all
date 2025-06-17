package com.example.etl.api.dto;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class UserPrincipalDTO {
    private String username;
    private List<String> roles;

    public UserPrincipalDTO(String username, Collection<String> roles) {
        this.username = username;
        this.roles = roles.stream().collect(Collectors.toList());
    }

    // Getters (Setters not strictly needed if only used for response)
    public String getUsername() { return username; }
    public List<String> getRoles() { return roles; }
}
