package com.icici;


import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.stereotype.Service;


@Service
@Configuration
@EnableWebSecurity
public class SecurityConfig extends WebSecurityConfigurerAdapter{
	

	@Override
    protected void configure(HttpSecurity http) throws Exception {
			http.authorizeRequests().antMatchers(HttpMethod.GET).permitAll();
			http.authorizeRequests().antMatchers(HttpMethod.POST).permitAll();
	        http.authorizeRequests().antMatchers(HttpMethod.DELETE).denyAll();
	        http.authorizeRequests().antMatchers(HttpMethod.PATCH).denyAll();
	        http.authorizeRequests().antMatchers(HttpMethod.PUT).denyAll();
	        http.authorizeRequests().antMatchers(HttpMethod.OPTIONS).denyAll();
	        
	     
	    
}
	
}
