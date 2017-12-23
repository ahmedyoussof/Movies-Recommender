package com.recommendation.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.recommendation.data.ConnectionManager;
import com.recommendation.data.MovieDAO;
import com.recommendation.data.MovieDAOImpl;
import com.recommendation.model.Movie;
import com.recommendation.model.Rating;
import com.recommendation.utils.FileResource;

public class MovieServiceImpl implements MovieService {

    private MovieDAO movieDAO = new MovieDAOImpl(ConnectionManager.getInstance());

    @Override
    public Movie findMovie(int id) {
        return movieDAO.findById(id);
    }

    @Override
    public boolean insertMovie(Movie movie) {
        return movieDAO.insert(movie);
    }

    @Override
    public boolean updateMovie(Movie movie) {
        return movieDAO.update(movie);
    }

    @Override
    public boolean deleteMovie(int id) {
        return movieDAO.delete(id);
    }

    @Override
    public List<Movie> listMovies() {
        return movieDAO.listMovies();
    }

    @Override
    public List<Movie> listRaterMovies(int raterId) {
        return movieDAO.listRaterMovies(raterId);
    }

    @Override
    public List<Movie> listCountryMovies(String country) {
        return movieDAO.listCountryMovies(country);
    }

    @Override
    public List<Movie> listDirectorMovies(String director) {
        return movieDAO.listDirectorMovies(director);
    }

    @Override
    public List<Movie> listGenreMovie(String genre) {
        return movieDAO.listGenreMovies(genre);
    }

    @Override
    public List<Movie> listCountryGenreMovies(String country, String genre) {
        return movieDAO.listCountryGenreMovies(country, genre);
    }

    @Override
    public List<Rating> findMovieRatings(int movieId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> listMovieDirectors(int movieId) {
        return movieDAO.findMovieDirectors(movieId);
    }

    @Override
    public boolean insertMovieDirector(int movieId, String director) {
        return movieDAO.insertMovieDirector(movieId, director);
    }

    @Override
    public boolean deleteMovieDirector(int movieId, String director) {
        return movieDAO.deleteMovieDirector(movieId, director);
    }

    @Override
    public List<Movie> listAllMovies() {
        return movieDAO.listMovies();
    }

    @Override
    public List<String> listMovieGenres(int movieId) {
        return movieDAO.findMovieGenres(movieId);
    }

    @Override
    public List<String> listMovieShowPlaces(int movieId) {
        return movieDAO.findMovieShowPlaces(movieId);
    }

    @Override
    public boolean insertMovieGenre(int movieId, String genre) {
        return movieDAO.insertMovieGenre(movieId, genre);
    }

    @Override
    public boolean insertMovieShowPlace(int movieId, String country) {
        return movieDAO.insertMovieShowPlace(movieId, country);
    }

    @Override
    public boolean deleteMovieGenre(int movieId, String genre) {
        return movieDAO.deleteMovieGenre(movieId, genre);
    }

    @Override
    public boolean deleteMovieShowPlace(int movieId, String country) {
        return movieDAO.deleteMovieShowPlace(movieId, country);
    }

    @Override
    public boolean importMovies(String fileName) {
        FileResource fr = new FileResource(fileName);
        CSVParser parser = fr.getCSVParser();
        List<Movie> movies = new ArrayList<>();

        for (CSVRecord rec : parser) {
            Movie mov = getCSVMovie(rec);
            mov.setDirectors(new HashSet<>(Arrays.asList(rec.get("director").split(", "))));
            mov.setGenres(new HashSet<>(Arrays.asList(rec.get("genre").split(", "))));
            mov.setShowPlaces(new HashSet<>(Arrays.asList(rec.get("country").split(", "))));
            movies.add(mov);
        }

        return movieDAO.batchInsertMovies(movies);
    }

    private Movie getCSVMovie(CSVRecord rec) {
        Movie mov = new Movie();
        mov.setId(Integer.parseInt(rec.get("id")));
        mov.setTitle(rec.get("title"));
        mov.setYear(Integer.parseInt(rec.get("year")));
        mov.setDuration(Integer.parseInt(rec.get("minutes")));
        mov.setPosterImage(rec.get("poster"));
        return mov;
    }

}
