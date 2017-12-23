package com.recommendation.data;

import java.util.List;
import com.recommendation.model.Rating;

public interface RaterDAO extends AbstractDAO<Rating, Rating> {
    
    public boolean batchInsertRatings(List<Rating> ratings);

    /**
     * list all movies ratings by movie_id
     * 
     * @param movieId refers to movie_id
     * @return list of Rating
     */
    public List<Rating> listMovieRatings(int movieId);
    
}
