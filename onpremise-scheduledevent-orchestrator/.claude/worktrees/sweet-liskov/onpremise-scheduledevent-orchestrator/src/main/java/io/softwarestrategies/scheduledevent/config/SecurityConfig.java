package io.softwarestrategies.scheduledevent.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Security configuration.
 *
 * Admin endpoints under /api/v1/events/admin/** require HTTP Basic Auth.
 * All other endpoints are publicly accessible.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

	@Value("${app.admin.username}")
	private String adminUsername;

	@Value("${app.admin.password}")
	private String adminPassword;

	@Bean
	public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
		http
				.csrf(AbstractHttpConfigurer::disable)
				.sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
				.authorizeHttpRequests(auth -> auth
						.requestMatchers("/api/v1/events/admin/**").hasRole("ADMIN")
						.anyRequest().permitAll()
				)
				.httpBasic(Customizer.withDefaults());
		return http.build();
	}

	@Bean
	public UserDetailsService userDetailsService(PasswordEncoder passwordEncoder) {
		return new InMemoryUserDetailsManager(
				User.withUsername(adminUsername)
						.password(passwordEncoder.encode(adminPassword))
						.roles("ADMIN")
						.build()
		);
	}

	@Bean
	public PasswordEncoder passwordEncoder() {
		return new BCryptPasswordEncoder();
	}
}
