package com.recommendation.data;

import java.util.List;

import com.recommendation.model.Movie;

public interface MovieDAO extends AbstractDAO<Movie, Integer> {
    
    public List<Movie> listMovies();
    
    public boolean batchInsertMovies(List<Movie> movies);   
    
    
    /**
     * list all movies rated by rater
     * @param rater indicates rater
     * @return list of movies
     */
    
    public List<Movie> listRaterMovies(int raterId);

    /**
     * list all  countries of movies
     * @param country refers to show place
     * @return list of movies
     */
    public List<Movie> listCountryMovies(String country);

    /**
     * list all directors of movies
     * @param director refers to movie directors
     * @return list of movies
     */
    public List<Movie> listDirectorMovies(String director);

    /**
     * list all genres of movies
     * @param genre refers to movies genre
     * @return list of movies
     */
    public List<Movie> listGenreMovies(String genre);

    /**
     * list movies by countries and genres
     * @param country refers to show place
     * @param genre refers to movie genres
     * @return list of movies
     */
    public List<Movie> listCountryGenreMovies(String country, String genre);
    
    /**
     * find all directors of movie
     * @param movieId refers to movie id
     * @return list of directors
     */
    public List<String> findMovieDirectors(Integer movieId);
    
    /**
     * find all genres of movie
     * @param movieId refers to movie id
     * @return list of genres
     */
    public List<String> findMovieGenres(Integer movieId);
    
    /**
     * find all show places of movie
     * @param movieId refers to movie id
     * @return list of show places
     */
    public List<String> findMovieShowPlaces(Integer movieId);
    
    /**
     * insert director of movie
     * @param movieId refers to movie id
     * @param director refers to movie director
     * @return true if inserted
     */
    public boolean insertMovieDirector(Integer movieId, String director);
    
    /**
     * insert genre of movie
     * @param movieId refers to movie id
     * @param genre refers to movie genre
     * @return true if inserted
     */
    public boolean insertMovieGenre(Integer movieId, String genre);
    
    /**
     * insert show place of movie
     * @param movieId refers to movie id
     * @param country refers to movie show place
     * @return true if inserted
     */
    public boolean insertMovieShowPlace(Integer movieId, String country);
    
    /**
     * delete director of movie
     * @param movieId refers to movie id
     * @param country refers to movie director
     * @return true if deleted
     */
    public boolean deleteMovieDirector(Integer movieId, String director);
    
    /**
     * delete genre of movie
     * @param movieId refers to movie id
     * @param country refers to movie genre
     * @return true if deleted
     */
    public boolean deleteMovieGenre(Integer movieId, String genre);
    
    /**
     * delete show place of movie
     * @param movieId refers to movie id
     * @param country refers to movie show place
     * @return true if deleted
     */
    public boolean deleteMovieShowPlace(Integer movieId, String showPlace);

    

}
