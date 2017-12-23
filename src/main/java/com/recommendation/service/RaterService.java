package com.recommendation.service;

import java.util.List;

import com.recommendation.model.Rating;

public interface RaterService {

    /**
     * find rating by id
     * 
     * @param id refers to rater_id
     * @return rating
     */
    public Rating findById(Rating id);
    
    /**
     * insert rating to ratings table
     * 
     * @param rating identifies table row
     * @return true if inserted
     */
    public boolean insert(Rating rating);
    
    /**
     * update row into ratings table 
     * 
     * @param rating identifies table row
     * @return true if updated
     */
    public boolean update(Rating rating);
    
    /**
     * delete row from movie table 
     * 
     * @param rating identifies table row
     * @return true if deleted
     */
    public boolean delete(Rating rating);
    
    /**
     * import movies from CSV file
     * 
     * @param fileName refers to CSV file name
     * @return true if imported
     */
    public boolean importRatings(String fileName);
    
    /**
     * list all movie ratings by raters
     * 
     * @param movieId indicates movie_id
     * @return list of rating
     */
    public List<Rating> listMovieRatings(int movieId);
    
}
