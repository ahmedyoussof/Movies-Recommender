package com.recommendation.model;

import java.util.HashSet;
import java.util.Set;

public class Movie {
	private Integer id;
	private Integer year;
	private Integer duration;
	private String title;
	private String posterImage;
	
	private Set<String> directors = new HashSet<>();
	private Set<String> genres = new HashSet<>();
	private Set<String> showPlaces= new HashSet<>();
	
	

	public Movie() {
    }

    public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Integer getDuration() {
		return duration;
	}

	public void setDuration(Integer duration) {
		this.duration = duration;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getPosterImage() {
		return posterImage;
	}

	public void setPosterImage(String posterImage) {
		this.posterImage = posterImage;
	}

    public Set<String> getDirectors() {
        return directors;
    }

    public void setDirectors(Set<String> directors) {
        this.directors = directors;
    }

    public Set<String> getGenres() {
        return genres;
    }

    public void setGenres(Set<String> genres) {
        this.genres = genres;
    }

    public Set<String> getShowPlaces() {
        return showPlaces;
    }

    public void setShowPlaces(Set<String> showPlaces) {
        this.showPlaces = showPlaces;
    }
	
	public void addDirector(String director) {	    
	    directors.add(director);
	}
	
   public void addGenre(String genre) {
        genres.add(genre);
    }

    public void addShowplace(String country) {
        showPlaces.add(country);
    }

}
