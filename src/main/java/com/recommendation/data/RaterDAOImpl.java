package com.recommendation.data;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.recommendation.model.Rating;

public class RaterDAOImpl implements RaterDAO {

    private static final String SELECT_RATING = "SELECT * FROM ratings WHERE rater_id = ? and movie_id = ?";
    private static final String INSERT_RATING = "INSERT INTO ratings  VALUES (?, ?, ?)";
    private static final String DELETE_RATING = "DELETE FROM ratings WHERE rater_id = ? and movie_id = ?";
    private static final String UPDATE_RATING = "UPDATE ratings SET value = ? WHERE rater_id = ? and movie_id = ?";
    private static final String SELECT_MOVIE_RATINGS = "SELECT * FROM ratings WHERE movie_id = ?";

    ConnectionManager connManager;

    public RaterDAOImpl(ConnectionManager connManager) {
        this.connManager = connManager;
    }

    @Override
    public Rating findById(Rating id) {
        Rating rating = null;
        try {
            PreparedStatement stmt = connManager.getConnection().prepareStatement(SELECT_RATING);
            stmt.setInt(1, id.getRaterId());
            stmt.setInt(2, id.getMovieId());
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) 
                 rating = getRatingFromResultSet(rs);
            
            connManager.close();
            return rating;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("RatingDAO.findById() Error: " + e.getMessage());
            return null;
        }
    }

    @Override
    public boolean batchInsertRatings(List<Rating> ratings) {
        try {
            // begin transaction
            connManager.getConnection().setAutoCommit(false);
            PreparedStatement insertRating = connManager.getConnection().prepareStatement(INSERT_RATING);
            for(Rating rating : ratings) {
                insertRating.setInt(1, rating.getRaterId());
                insertRating.setInt(2, rating.getMovieId());
                insertRating.setInt(3, rating.getValue());
                insertRating.addBatch();
            }
            insertRating.executeBatch();
            // commit transaction
            connManager.getConnection().commit();
            connManager.close();
            return true;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("RaterDAO.batchInsertRatings() Error: " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean insert(Rating rating) {
        try {
            connManager.getConnection().setAutoCommit(false);
            PreparedStatement insertRating = connManager.getConnection().prepareStatement(INSERT_RATING);
            insertRating.setInt(1, rating.getRaterId());
            insertRating.setInt(2, rating.getMovieId());
            insertRating.setInt(3, rating.getValue());
            insertRating.executeUpdate();
            
            connManager.getConnection().commit();
            connManager.close();
            return true;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("RaterDAO.insert() Error: " + e.getMessage());
            return false;
        } 
    }

    @Override
    public boolean update(Rating rating) {
        try {
            //begin transaction
            connManager.getConnection().setAutoCommit(false);
            PreparedStatement stmt = connManager.getConnection().prepareStatement(UPDATE_RATING);
            stmt.setInt(1, rating.getValue());
            stmt.setInt(2, rating.getRaterId());
            stmt.setInt(3, rating.getMovieId());
            stmt.executeUpdate();
            connManager.getConnection().commit();
            connManager.close();
            return true;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("RatingDAO.update() Error: " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean delete(Rating rating) {
        try {
            PreparedStatement stmt = connManager.getConnection().prepareStatement(DELETE_RATING);
            stmt.setInt(1, rating.getRaterId());
            stmt.setInt(2, rating.getMovieId());
            stmt.executeUpdate();
            connManager.close();
            return true;
        } catch (Exception e) {
            connManager.close();
            System.out.println("RatingDAO.delete() Error: " + e.getMessage());
            return false;
        }
    }

    @Override
    public List<Rating> listMovieRatings(int movieId) {
        try {
            PreparedStatement stmt = connManager.getConnection().prepareStatement(SELECT_MOVIE_RATINGS);
            stmt.setInt(1, movieId);
            
            ResultSet rs = stmt.executeQuery();
            List<Rating> ratingsList = getRatingsFromResultSet(rs);
            
            connManager.close();
            return ratingsList;
        } catch (SQLException e) {
            connManager.close();
            System.out.println("RaterDAO.listMovieRatings() Error: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    private Rating getRatingFromResultSet(ResultSet rs) throws SQLException {
        Rating rating = new Rating();
        rating.setRaterId(rs.getInt("rater_id"));
        rating.setMovieId(rs.getInt("movie_id"));
        rating.setValue(rs.getInt("value"));
        return rating;
    }

    private List<Rating> getRatingsFromResultSet(ResultSet rs) throws SQLException {
        List<Rating> ratings = new ArrayList<>();
        while (rs.next()) {
            Rating rating = getRatingFromResultSet(rs);
            ratings.add(rating);
        }
        return ratings;
    }
}