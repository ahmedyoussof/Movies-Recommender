package com.recommedation.test.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.recommendation.data.ConnectionManager;
import com.recommendation.data.MovieDAO;
import com.recommendation.data.MovieDAOImpl;
import com.recommendation.model.Movie;

public class MovieDAOTest {
    private ConnectionManager connManager = ConnectionManager.getInstance();
    private MovieDAO movieDAO = new MovieDAOImpl(connManager);
    
    @Test
    public void testFindMovieById() {
        Movie movie = movieDAO.findById(6414);
        assertNotNull(movie);
        assertEquals("Test movie Title: ","Behind the Screen", movie.getTitle());
        assertEquals("Test movie Year: ",1916 , movie.getYear().intValue());
        assertEquals("Test movie Duration: ",30 , movie.getDuration().intValue());
        assertEquals("Test movie Poster: ",
                "http://ia.media-imdb.com/images/M/MV5BMTkyNDYyNTczNF5BMl5BanBnXkFtZTgwMDU2MzAwMzE@._V1_SX300.jpg",
                movie.getPosterImage());
        assertEquals("Test movie Countries: ", 1, movie.getShowPlaces().size());
        assertEquals("Test movie genres: ", 3, movie.getGenres().size());
        assertEquals("Test movie genres: ", 1, movie.getDirectors().size());
    }

}
