import React, { createContext, useContext, useState, ReactNode, useCallback } from 'react';
import apiClient from '../services/apiClient';

interface User {
  username: string;
  roles: string[];
}

interface AuthContextType {
  isAuthenticated: boolean;
  user: User | null;
  login: (usernameInput: string, passwordInput: string) => Promise<void>;
  logout: () => void;
  isLoading: boolean; // To indicate login process activity
  authError: string | null; // To store login error messages
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [isAuthenticated, setIsAuthenticated] = useState<boolean>(false);
  const [user, setUser] = useState<User | null>(null);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [authError, setAuthError] = useState<string | null>(null);

  // Function to set the Authorization header for all apiClient requests
  const setAuthHeader = (usernameInput: string, passwordInput: string) => {
    const basicAuth = 'Basic ' + btoa(usernameInput + ':' + passwordInput);
    apiClient.defaults.headers.common['Authorization'] = basicAuth;
  };

  const clearAuthHeader = () => {
    delete apiClient.defaults.headers.common['Authorization'];
  };

  const login = useCallback(async (usernameInput: string, passwordInput: string) => {
    setIsLoading(true);
    setAuthError(null);
    try {
      setAuthHeader(usernameInput, passwordInput); // Set header for the /users/me call
      const response = await apiClient.get<{ username: string; roles: string[] }>('/users/me');
      setUser(response.data);
      setIsAuthenticated(true);
      // Persist basic auth for subsequent requests in this session by keeping it in apiClient defaults
    } catch (error: any) {
      console.error('Login failed:', error);
      clearAuthHeader(); // Clear header on failed login
      setAuthError(error.response?.data?.message || error.message || 'Login failed. Please check credentials.');
      setIsAuthenticated(false);
      setUser(null);
      throw error; // Re-throw to allow form to handle it
    } finally {
      setIsLoading(false);
    }
  }, []);

  const logout = useCallback(() => {
    clearAuthHeader();
    setUser(null);
    setIsAuthenticated(false);
    setAuthError(null);
    // Optionally, redirect to login page or home page via navigate from react-router-dom if needed here
    // For now, just clears state. Navigation can be handled by components consuming this.
    console.log('User logged out');
  }, []);

  return (
    <AuthContext.Provider value={{ isAuthenticated, user, login, logout, isLoading, authError }}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = (): AuthContextType => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
