package com.recommendation.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.recommendation.data.ConnectionManager;
import com.recommendation.data.RaterDAO;
import com.recommendation.data.RaterDAOImpl;
import com.recommendation.model.Rating;
import com.recommendation.utils.FileResource;

public class RaterServiceImpl implements RaterService{

    private RaterDAO  raterDAO = new RaterDAOImpl(ConnectionManager.getInstance()); 

    @Override
    public Rating findById(Rating id) {
        return raterDAO.findById(id);
    }
    
    @Override
    public boolean insert(Rating rating) {
        return raterDAO.insert(rating);
    }

    @Override
    public boolean update(Rating rating) {
        return raterDAO.update(rating);
    }

    @Override
    public boolean delete(Rating rating) {
        return raterDAO.delete(rating);
    }

    @Override
    public List<Rating> listMovieRatings(int movieId) {
        return raterDAO.listMovieRatings(movieId);
    }
    
    @Override
    public boolean importRatings(String filename) {
        FileResource fr = new FileResource(filename);
        CSVParser parser = fr.getCSVParser();
        List<Rating> ratings = new ArrayList<>();
        for(CSVRecord rec : parser) {
            Rating rating = getCSVRating(rec);
            ratings.add(rating);           
        }
        return raterDAO.batchInsertRatings(ratings);
    }
    
    private Rating getCSVRating(CSVRecord rec) {
        Rating rating = new Rating();
        rating.setRaterId(Integer.parseInt(rec.get("rater_id")));
        rating.setMovieId(Integer.parseInt(rec.get("movie_id")));
        rating.setValue(Integer.parseInt(rec.get("rating")));
        return rating;
    }
}
