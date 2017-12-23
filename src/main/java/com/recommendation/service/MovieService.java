package com.recommendation.service;

import java.util.List;

import com.recommendation.model.Movie;
import com.recommendation.model.Rating;

public interface MovieService {
    
    /**
     * find movie by id
     * 
     * @param id refers to movie id
     * @return movie
     */
    public Movie findMovie(int id);

    /**
     * insert row into movie table
     * 
     * @param movie identifies table row
     * @return true if inserted
     */
    public boolean insertMovie(Movie movie);

    /**
     * update row into movie table 
     * 
     * @param movie identifies table row 
     * @return true if updated
     */
    public boolean updateMovie(Movie movie);

    /**
     * delete row from movie table 
     * 
     * @param id refers to movie id
     * @return true if deleted
     */
    public boolean deleteMovie(int id);
    
    public List<Movie> listMovies();

    /**
     * list all movies rated by rater
     * 
     * @param raterId indicates rater
     * @return list of movies
     */
    
    public List<Movie> listRaterMovies(int raterId);

    /**
     * list all  countries of movies
     * 
     * @param country refers to show place
     * @return list of movies
     */
    public List<Movie> listCountryMovies(String country);

    /**
     * list all directors of movies
     * 
     * @param director refers to movie directors
     * @return list of movies
     */
    public List<Movie> listDirectorMovies(String director);

    /**
     * list all genres of movies
     * 
     * @param genre refers to movies genre
     * @return list of movies
     */
    public List<Movie> listGenreMovie(String genre);

    /**
     * list movies by countries and genres
     * 
     * @param country refers to show place
     * @param genre refers to movie genres
     * @return list of movies
     */
    public List<Movie> listCountryGenreMovies(String country, String genre);
    
    /**
     * list rating of movie
     * 
     * @param movieId refers to movie id
     * @return list of ratings
     */
    public List<Rating> findMovieRatings(int movieId);

    /**
     * list directors of movie
     * 
     * @param movieId refers to movie_id
     * @return list of String
     */
    public List<String> listMovieDirectors(int movieId);
    
    /**
     * insert director to movie
     * 
     * @param movieId refers to movie_id
     * @param director refers to director name
     * @return true if inserted
     */
    public boolean insertMovieDirector(int movieId, String director);
    
    /**
     * delete director from movie
     * 
     * @param movieId refers to movie_id
     * @param director refers to director name
     * @return true if deleted
     */
    public boolean deleteMovieDirector(int movieId, String director);
    
    /**
     * import movies from CSV file
     * 
     * @param fileName refers to CSV file name
     * @return true if imported
     */
    public boolean importMovies(String fileName);

    /**
     * list all movies
     * 
     * @return list of all movies
     */
    public List<Movie> listAllMovies();

    /**
     * find all genres of movie
     * 
     * @param movieId refers to movie id
     * @return list of genres
     */
    public List<String> listMovieGenres(int movieId);
    
    /**
     * find all show places of movie
     * 
     * @param movieId refers to movie id
     * @return list of show places
     */
    public List<String> listMovieShowPlaces(int movieId);
    
    /**
     * insert genre of movie
     * 
     * @param movieId refers to movie id
     * @param genre refers to movie genre
     * @return true if inserted
     */
    public boolean insertMovieGenre(int movieId, String genre);

    /**
     * insert show place of movie
     * 
     * @param movieId refers to movie id
     * @param country refers to movie show place
     * @return true if inserted
     */
    public boolean insertMovieShowPlace(int movieId, String country);

    /**
     * delete genre of movie
     * 
     * @param movieId refers to movie id
     * @param country refers to movie genre
     * @return true if deleted
     */
    public boolean deleteMovieGenre(int movieId, String genre);

    /**
     * delete show place of movie
     * 
     * @param movieId refers to movie id
     * @param country refers to movie show place
     * @return true if deleted
     */
    public boolean deleteMovieShowPlace(int movieId, String country);
}
