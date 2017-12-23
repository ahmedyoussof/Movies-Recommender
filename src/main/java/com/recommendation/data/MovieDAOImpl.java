package com.recommendation.data;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.recommendation.model.Movie;

public class MovieDAOImpl implements MovieDAO {

    private static final String SELECT_MOVIES = "SELECT m.*,d.director,g.genre,sp.country FROM movies m"
            + " JOIN directings d on m.id = d.movie_id" + " JOIN genres g ON m.id = g.movie_id "
            + " JOIN showplaces sp on m.id = sp.movie_id";
    

    private static final String SELECT_MOVIE_BY_ID = SELECT_MOVIES + " WHERE id = ?";
    private static final String INSERT_MOVIE = "INSERT into movies  VALUES (?, ?, ?, ?, ?)";
    private static final String UPDATE_MOVIE = "UPDATE movies SET TITLE = ?, YEAR = ?, DURATION = ?, poster = ? WHERE ID = ?";
    private static final String DELETE_MOVIE = "DELETE  FROM movies  WHERE ID = ?";
    
    private static final String SELECT_RATER_MOVIES = SELECT_MOVIES + " JOIN ratings r ON m.id = r.movie_id WHERE rater_id = ?";

    private static final String SELECT_DIRECTOR_MOVIES = SELECT_MOVIES + " WHERE director = ?";
    private static final String SELECT_COUNTRY_MOVIES = SELECT_MOVIES +" WHERE country = ?";
    private static final String SELECT_GENRE_MOVIES = SELECT_MOVIES +" WHERE genre = ?";
    
    
    private static final String SELECT_COUNTRY_GENRE_MOVIES = SELECT_MOVIES + " WHERE country = ? AND genre = ?";
    
    private static final String SELECT_MOVIE_GENRES = "SELECT genre FROM genres WHERE movie_id = ?";
    private static final String INSERT_MOVIE_GENRE = "INSERT INTO genres VALUES (?, ?)";
    private static final String DELETE_MOVIE_GENRE = "DELETE  FROM genres  WHERE movie_id = ?";
    
    private static final String SELECT_MOVIE_DIRECTORS = "SELECT * FROM directings WHERE movie_id = ?";
    private static final String INSERT_MOVIE_DIRECTOR = "INSERT INTO directings VALUES (?, ?)";
    private static final String DELETE_DIRECTOR = "DELETE FROM directings WHERE movie_id = ? and director = ?";
    
    private static final String SELECT_MOVIE_SHOWPLACES = "SELECT country FROM showplaces WHERE movie_id = ?";
    private static final String INSERT_MOVIE_SHOWPLACE = "INSERT INTO showplaces VALUES (?, ?)";    
    private static final String DELETE_MOVIE_SHOWPLACE = "DELETE  FROM Showplaces  WHERE movie_id = ?";


    



    private ConnectionManager connManager;

    public MovieDAOImpl(ConnectionManager connManager) {
        this.connManager = connManager;
    }

    public Movie findById(Integer id) {

        Movie movie = null;
        
        PreparedStatement stmt;
        try {
            stmt = connManager.getConnection().prepareStatement(SELECT_MOVIE_BY_ID, ResultSet.CONCUR_READ_ONLY);
            stmt.setInt(1, id);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                if(movie == null)
                    movie = getMovieFromResultSet(rs);               
                
                movie.addDirector(rs.getString("director"));
                movie.addShowplace(rs.getString("country"));
                movie.addGenre(rs.getString("genre"));
                
            }
            connManager.close();
            return movie;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.findById() Error: " + e.getMessage());
            return null;
        }

    }

    @Override
    public boolean batchInsertMovies(List<Movie> movies) {
        try {
            // begin transaction
            connManager.getConnection().setAutoCommit(false);

            PreparedStatement insertMovie = connManager.getConnection().prepareStatement(INSERT_MOVIE);
            PreparedStatement insertDirecting = connManager.getConnection().prepareStatement(INSERT_MOVIE_DIRECTOR);
            PreparedStatement insertGenres = connManager.getConnection().prepareStatement(INSERT_MOVIE_GENRE);
            PreparedStatement insertShowPlace = connManager.getConnection().prepareStatement(INSERT_MOVIE_SHOWPLACE);

            for (Movie movie : movies) {
                insertMovie.setInt(1, movie.getId());
                insertMovie.setString(2, movie.getTitle());
                insertMovie.setInt(3, movie.getYear());
                insertMovie.setInt(4, movie.getDuration());
                insertMovie.setString(5, movie.getPosterImage());
                insertMovie.addBatch();

                for (String director : movie.getDirectors()) {
                    insertDirecting.setInt(1, movie.getId());
                    insertDirecting.setString(2, director);
                    insertDirecting.addBatch();
                }

                for (String genre : movie.getGenres()) {
                    insertGenres.setInt(1, movie.getId());
                    insertGenres.setString(2, genre);
                    insertGenres.addBatch();
                }

                for (String country : movie.getShowPlaces()) {
                    insertShowPlace.setInt(1, movie.getId());
                    insertShowPlace.setString(2, country);
                    insertShowPlace.addBatch();
                }
            }

            insertMovie.executeBatch();
            insertDirecting.executeBatch();
            insertGenres.executeBatch();
            insertShowPlace.executeBatch();
            // commit transaction
            connManager.getConnection().commit();
            connManager.close();
            return true;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.batchInsertMovies() Error: " + e.getMessage());
            return false;
        }
    }

    public boolean insert(Movie movie) {

        try {
            // begin transaction
            connManager.getConnection().setAutoCommit(false);
            
            PreparedStatement insertMovie = connManager.getConnection().prepareStatement(INSERT_MOVIE);
            PreparedStatement insertDirecting = connManager.getConnection().prepareStatement(INSERT_MOVIE_DIRECTOR);
            PreparedStatement insertGenres = connManager.getConnection().prepareStatement(INSERT_MOVIE_GENRE);
            PreparedStatement insertShowPlace = connManager.getConnection().prepareStatement(INSERT_MOVIE_SHOWPLACE);

            insertMovie.setInt(1, movie.getId());
            insertMovie.setString(2, movie.getTitle());
            insertMovie.setInt(3, movie.getYear());
            insertMovie.setInt(4, movie.getDuration());
            insertMovie.setString(5, movie.getPosterImage());
            insertMovie.executeUpdate();

            for (String director : movie.getDirectors()) {
                insertDirecting.setInt(1, movie.getId());
                insertDirecting.setString(2, director);
                insertDirecting.addBatch();
            }
            insertDirecting.executeBatch();

            for (String genre : movie.getGenres()) {
                insertGenres.setInt(1, movie.getId());
                insertGenres.setString(2, genre);
                insertGenres.addBatch();
            }
            insertGenres.executeBatch();

            for (String country : movie.getShowPlaces()) {
                insertShowPlace.setInt(1, movie.getId());
                insertShowPlace.setString(2, country);
                insertShowPlace.addBatch();
            }
            insertShowPlace.executeBatch();
            
            connManager.getConnection().commit();
            connManager.close();
            return true;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.insert() Error: " + e.getMessage());
            return false;
        }

    }

    public boolean delete(Integer id) {
        try {
            PreparedStatement stmt = connManager.getConnection().prepareStatement(DELETE_MOVIE);
            stmt.setInt(1, id);
            stmt.executeUpdate();
            connManager.close();
            return true;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.delete() Error: " + e.getMessage());
            return false;
        }

    }

    public boolean update(Movie entity) {
        try {
            //begin transaction
            connManager.getConnection().setAutoCommit(false);
            PreparedStatement stmt = connManager.getConnection().prepareStatement(UPDATE_MOVIE);
            stmt.setString(1, entity.getTitle());
            stmt.setInt(2, entity.getYear());
            stmt.setInt(3, entity.getDuration());
            stmt.setString(4, entity.getPosterImage());
            stmt.setInt(5, entity.getId());
            stmt.executeUpdate();
            connManager.getConnection().setAutoCommit(false);
            connManager.close();
            return true;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.update() Error: " + e.getMessage());
            return false;
        }
    }

    public List<Movie> listRaterMovies(int raterId) {
        try {
            PreparedStatement stmt = connManager.getConnection().prepareStatement(SELECT_RATER_MOVIES);
            stmt.setInt(1, raterId);
            ResultSet rs = stmt.executeQuery();
            
            List<Movie> movies = getMoviesFromResultSet(rs);
            
            connManager.close();
            return movies;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.listRaterMovies() Error: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    public List<Movie> listCountryMovies(String country) {
        List<Movie> moviesList = new ArrayList<>();
        PreparedStatement stmt;
        try {
            stmt = connManager.getConnection().prepareStatement(SELECT_COUNTRY_MOVIES);
            stmt.setString(1, country);
            ResultSet rs = stmt.executeQuery();
            moviesList = getMoviesFromResultSet(rs);            
            connManager.close();
            
            return moviesList;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.listCountryMovies() Error: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    public List<Movie> listDirectorMovies(String director) {
        List<Movie> moviesList = new ArrayList<>(); 
        PreparedStatement stmt;
        try {
            stmt = connManager.getConnection().prepareStatement(SELECT_DIRECTOR_MOVIES);
            stmt.setString(1, director);
            ResultSet rs = stmt.executeQuery();
            moviesList = getMoviesFromResultSet(rs);            
            connManager.close();
            
            return moviesList;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.listDirectorMovies() Error: " + e.getMessage());
            return new ArrayList<>();
        }

    }

    public List<Movie> listGenreMovies(String genre) {
        try {
            PreparedStatement stmt = connManager.getConnection().prepareStatement(SELECT_GENRE_MOVIES);
            stmt.setString(1, genre);
            ResultSet rs = stmt.executeQuery();
            
            List<Movie> moviesList = getMoviesFromResultSet(rs);
            
            connManager.close();
            return moviesList;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.listGenreMovies() Error: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    public List<Movie> listCountryGenreMovies(String country, String genre) {

        List<Movie> moviesList = new ArrayList<Movie>();
        
        PreparedStatement stmt;

        try {
            stmt = connManager.getConnection().prepareStatement(SELECT_COUNTRY_GENRE_MOVIES);
            stmt.setString(1, country);
            stmt.setString(2, genre);
            ResultSet rs = stmt.executeQuery();
            moviesList = getMoviesFromResultSet(rs);
            
            connManager.close();
            return moviesList;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.listCountryGenreMovies() Error: " + e.getMessage());
            return new ArrayList<>();
        }

    }
 
    @Override
    public List<Movie> listMovies() {

        List<Movie> moviesList = new ArrayList<Movie>();
        PreparedStatement stmt;
        try {
            stmt = connManager.getConnection().prepareStatement(SELECT_MOVIES);
            ResultSet rs = stmt.executeQuery();

            moviesList = getMoviesFromResultSet(rs);
            connManager.close();
            return moviesList;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.listMovies() Error: " + e.getMessage());
            return null;
        }
    }

    @Override
    public List<String> findMovieDirectors(Integer movieId) {
        try {
            PreparedStatement stmt = connManager.getConnection().prepareStatement(SELECT_MOVIE_DIRECTORS);
            stmt.setInt(1, movieId);
            ResultSet rs = stmt.executeQuery();
            
            List<String> movieDirectors = new ArrayList<>();
            List<Movie> moviesList = getMoviesFromResultSet(rs);
            
            for(Movie mov : moviesList) {
                movieDirectors.addAll(mov.getDirectors());
            }
            connManager.close();
            return movieDirectors;  
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.findMovieDirectors() Error: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    @Override
    public List<String> findMovieGenres(Integer movieId) {

        
        List<String> movieGenres = new ArrayList<>();
        
        PreparedStatement stmt;
        try {
            stmt = connManager.getConnection().prepareStatement(SELECT_MOVIE_GENRES);
            stmt.setInt(1, movieId);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                
                movieGenres.add(rs.getString("genre"));
            
            }
            connManager.close();
            return movieGenres;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.findMovieGenres() Error: " + e.getMessage());
            return null;
        }
    }

    @Override
    public List<String> findMovieShowPlaces(Integer movieId) {

        List<String> movieGenres = new ArrayList<>();
        
        PreparedStatement stmt;
        try {
            stmt = connManager.getConnection().prepareStatement(SELECT_MOVIE_SHOWPLACES);
            stmt.setInt(1, movieId);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                
                movieGenres.add(rs.getString("country"));
            
            }
            connManager.close();
            return movieGenres;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.findMovieShowPlaces() Error: " + e.getMessage());
            return null;
        }
    }

    @Override
    public boolean insertMovieDirector(Integer movieId, String director) {
        try {
            PreparedStatement stmt = connManager.getConnection().prepareStatement(INSERT_MOVIE_DIRECTOR);
            stmt.setInt(1, movieId);
            stmt.setString(2, director);
            
            stmt.executeUpdate();
            connManager.close();
            return true;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.insertMovieDirector() Error: " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean insertMovieGenre(Integer movieId, String genre) {

        PreparedStatement stmt;
        try {

            stmt = connManager.getConnection().prepareStatement(INSERT_MOVIE_GENRE);
            stmt.setInt(1, movieId);
            stmt.setString(2, genre);
            stmt.executeUpdate();

            connManager.close();
            return true;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.insertMovieGenre() Error: " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean insertMovieShowPlace(Integer movieId, String country) {
        
        PreparedStatement stmt;
        try {

            stmt = connManager.getConnection().prepareStatement(INSERT_MOVIE_SHOWPLACE);
            stmt.setInt(1, movieId);
            stmt.setString(2, country);
            stmt.executeUpdate();

            connManager.close();
            return true;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.insertMovieShowPlace() Error: " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean deleteMovieDirector(Integer movieId, String director) {
        try {
            PreparedStatement stmt = connManager.getConnection().prepareStatement(DELETE_DIRECTOR);
            stmt.setInt(1, movieId);
            stmt.setString(2, director);
            stmt.executeUpdate();
            
            connManager.close();
            return true;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.deleteMovieDirector() Error: " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean deleteMovieGenre(Integer movieId, String genre) {
        
        PreparedStatement stmt;
        try {

            stmt = connManager.getConnection().prepareStatement(DELETE_MOVIE_GENRE);
            stmt.setInt(1, movieId);      
            stmt.executeUpdate();

            connManager.close();
            return true;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.deleteMovieGenre() Error: " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean deleteMovieShowPlace(Integer movieId, String showPlace) {

        PreparedStatement stmt;
        try {

            stmt = connManager.getConnection().prepareStatement(DELETE_MOVIE_SHOWPLACE);
            stmt.setInt(1, movieId);      
            stmt.executeUpdate();

            connManager.close();
            return true;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("MovieDAO.deleteMovieShowPlace() Error: " + e.getMessage());
            return false;
        }
    }

    private Movie getMovieFromResultSet(ResultSet rs) throws SQLException {
        Movie movie = new Movie();
        movie.setId(rs.getInt("id"));
        movie.setTitle(rs.getString("title"));
        movie.setYear(rs.getInt("year"));
        movie.setDuration(rs.getInt("duration"));
        movie.setPosterImage(rs.getString("poster"));
        return movie;
    }
    
    private List<Movie> getMoviesFromResultSet(ResultSet rs) throws SQLException {
        List<Movie> movieList = new ArrayList<Movie>();
        HashMap<Integer, Movie> movieSMap = new HashMap<>();
        Movie movie = null;

        while (rs.next()) {
            int movieId = rs.getInt("id");
            if (!movieSMap.containsKey(movieId)) {
                movie = getMovieFromResultSet(rs);
                movieSMap.put(movieId, movie);
            } else {
                movie = movieSMap.get(movieId);
            }
            movie.addDirector(rs.getString("director"));
            movie.addGenre(rs.getString("genre"));
            movie.addShowplace(rs.getString("country"));
        }
        movieList.addAll(movieSMap.values());
        return movieList;
    }
}
