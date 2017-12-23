package com.recommendation.test;

import com.recommendation.model.Movie;
import com.recommendation.service.MovieService;
import com.recommendation.service.MovieServiceImpl;

public class Main {
    public static void main(String[] args) {
        MovieService service = new MovieServiceImpl();
        Movie m = service.findMovie(6414);
        System.out.println(m.getTitle());
    }
}
