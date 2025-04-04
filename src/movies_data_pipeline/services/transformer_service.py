import pandas as pd
from typing import Dict, List, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class Transformer:
    def __init__(self, bronze_path: str):
        """Initialize the Transformer.
        
        Args:
            bronze_path: Path to the bronze layer data
        """
        self.bronze_path = bronze_path
    
    def transform(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Transform raw data into silver and gold layer tables.
        
        Returns:
            Dictionary containing silver and gold layer dataframes
            
        Raises:
            KeyError: If required columns are missing
        """
        try:
            # Read raw data
            raw_df = pd.read_parquet(self.bronze_path)
            
            # Standardize columns
            raw_df = self._standardize_columns(raw_df)
            
            # Process date columns
            raw_df = self._process_dates(raw_df)
            
            # Process genre and crew
            raw_df = self._process_genre_and_crew(raw_df)
            
            # Create dimension tables
            dim_tables = self._create_dimension_tables(raw_df)
            
            # Create bridge tables
            bridge_tables = self._create_bridge_tables(raw_df, dim_tables)
            
            # Create fact tables
            fact_tables = self._create_fact_tables(raw_df, dim_tables)
            
            # Create gold layer tables
            gold_tables = self._create_gold_tables(fact_tables['factMoviePerformance'], 
                                                  bridge_tables['bridge_movie_genre'],
                                                  dim_tables)
            
            # Combine all tables
            result = {
                "silver": {**dim_tables, **bridge_tables, **fact_tables},
                "gold": gold_tables
            }
            
            logger.info("Transformation completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names in the dataframe.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame with standardized column names
        """
        if 'names' in df.columns and 'name' not in df.columns:
            df = df.rename(columns={'names': 'name'})
        elif 'names' in df.columns:
            df = df.drop(columns=['names'])
        
        if 'name' not in df.columns:
            raise KeyError("Input data must contain a 'name' column for movie titles.")
        
        return df
    
    def _process_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and standardize date columns.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame with standardized date columns
            
        Raises:
            KeyError: If no valid date column is found
        """
        possible_date_cols = ["date_x", "release_date", "date"]
        date_col = next((col for col in possible_date_cols if col in df.columns), None)
        
        if date_col:
            df = df.rename(columns={date_col: "date_x"})
            df["date_x"] = df["date_x"].str.strip()
            df["date_x"] = pd.to_datetime(df["date_x"], format="%m/%d/%Y", errors="coerce")

        else:
            raise KeyError("Input data must contain a date column ('date_x', 'release_date', or 'date')")
        
        return df
    
    def _process_genre_and_crew(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process genre and crew data.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame with processed genre and crew data
        """
        df["genre_list"] = df["genre"].str.split(",\s+")
        
        df["crew_pairs"] = df["crew"].apply(self._parse_crew)
        
        return df
    
    def _parse_crew(self, crew_str: str) -> List[Dict[str, str]]:
        """Parse crew string into structured data.
        
        Args:
            crew_str: Comma-separated string of crew members
            
        Returns:
            List of dictionaries with actor and character information
        """
        if pd.isna(crew_str) or crew_str == "":
            return []
            
        crew_list = crew_str.split(", ")
        pairs = []
        
        for i in range(0, len(crew_list), 2):
            if i + 1 < len(crew_list):
                pairs.append({"actor_name": crew_list[i], "character_name": crew_list[i + 1]})
            else:
                pairs.append({"actor_name": crew_list[i], "character_name": "Self"})
                
        return pairs
    
    def _create_dimension_tables(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create dimension tables from raw data.
        
        Args:
            df: Processed raw dataframe
            
        Returns:
            Dictionary of dimension tables
        """
        # Date dimension
        dim_date = df[["date_x"]].drop_duplicates().reset_index(drop=True)
        dim_date["date_id"] = dim_date.index + 1
        dim_date["year"] = dim_date["date_x"].dt.year
        dim_date["month"] = dim_date["date_x"].dt.month
        dim_date["day"] = dim_date["date_x"].dt.day
        dim_date["quarter"] = dim_date["date_x"].dt.quarter
        
        # Genre dimension
        genres = df["genre_list"].explode().dropna().unique()
        dim_genre = pd.DataFrame({"genre_name": genres})
        dim_genre["genre_id"] = dim_genre.index + 1
        
        # Language dimension
        dim_language = df[["orig_lang"]].drop_duplicates().reset_index(drop=True)
        dim_language.columns = ["language_name"]
        dim_language["language_id"] = dim_language.index + 1
        
        # Country dimension
        dim_country = df[["country"]].drop_duplicates().reset_index(drop=True)
        dim_country.columns = ["country_name"]
        dim_country["country_id"] = dim_country.index + 1
        
        # Movie dimension
        dim_movie = df.merge(dim_date, on="date_x", suffixes=('_raw', '_date')) \
                      .merge(dim_language, left_on="orig_lang", right_on="language_name") \
                      .merge(dim_country, left_on="country", right_on="country_name")
        dim_movie["movie_id"] = dim_movie.index + 1
        dim_movie = dim_movie[["movie_id", "name", "orig_title", "overview", "status", 
                              "crew_pairs", "date_x", "date_id", "language_id", "country_id", "genre_list"]]
        
        # Crew dimension
        crew_df = dim_movie[["movie_id", "crew_pairs"]].explode("crew_pairs").reset_index(drop=True)
        crew_df = crew_df.dropna(subset=["crew_pairs"])
        crew_df = pd.concat([crew_df["movie_id"], crew_df["crew_pairs"].apply(pd.Series)], axis=1)
        crew_df["role"] = "Actor"
        
        dim_crew = crew_df[["actor_name"]].drop_duplicates().reset_index(drop=True)
        dim_crew.columns = ["crew_name"]
        dim_crew["crew_id"] = dim_crew.index + 1
        
        # Role dimension
        dim_role = pd.DataFrame({"role": ["Actor"]}).reset_index(drop=True)
        dim_role["role_id"] = dim_role.index + 1
        
        return {
            "dim_date": dim_date,
            "dim_genre": dim_genre,
            "dim_language": dim_language,
            "dim_country": dim_country,
            "dim_movie": dim_movie,
            "dim_crew": dim_crew,
            "dim_role": dim_role
        }
    
    def _create_bridge_tables(self, raw_df: pd.DataFrame, dim_tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Create bridge tables between dimensions.
        
        Args:
            raw_df: Raw dataframe
            dim_tables: Dictionary of dimension tables
            
        Returns:
            Dictionary of bridge tables
        """
        # Movie-Genre bridge
        movie_genre_df = dim_tables["dim_movie"][["movie_id", "genre_list"]].explode("genre_list").rename(columns={"genre_list": "genre_name"})
        movie_genre_df = movie_genre_df.dropna(subset=["genre_name"])
        movie_genre_df = movie_genre_df[movie_genre_df["genre_name"] != ""]
        bridge_movie_genre = movie_genre_df.merge(dim_tables["dim_genre"], on="genre_name")
        bridge_movie_genre["movie_genre_id"] = bridge_movie_genre.index + 1
        bridge_movie_genre = bridge_movie_genre[["movie_genre_id", "movie_id", "genre_id"]]
        
        # Movie-Crew bridge
        crew_df = dim_tables["dim_movie"][["movie_id", "crew_pairs"]].explode("crew_pairs").reset_index(drop=True)
        crew_df = crew_df.dropna(subset=["crew_pairs"])
        crew_df = pd.concat([crew_df["movie_id"], crew_df["crew_pairs"].apply(pd.Series)], axis=1)
        crew_df["role"] = "Actor"
        
        bridge_movie_crew = crew_df.merge(dim_tables["dim_crew"], left_on="actor_name", right_on="crew_name") \
                                  .merge(dim_tables["dim_role"], on="role")
        bridge_movie_crew["movie_crew_id"] = bridge_movie_crew.index + 1
        bridge_movie_crew = bridge_movie_crew[["movie_crew_id", "movie_id", "crew_id", "role_id", "character_name"]]
        
        return {
            "bridge_movie_genre": bridge_movie_genre,
            "bridge_movie_crew": bridge_movie_crew
        }
    
    def _create_fact_tables(self, raw_df: pd.DataFrame, dim_tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Create fact tables.
        
        Args:
            raw_df: Raw dataframe
            dim_tables: Dictionary of dimension tables
            
        Returns:
            Dictionary of fact tables
        """
        fact_movie_performance = raw_df.merge(dim_tables["dim_movie"], on=["name", "date_x", "orig_title"])
        fact_movie_performance["financial_id"] = fact_movie_performance.index + 1
        fact_movie_performance["profit"] = fact_movie_performance["revenue"] - fact_movie_performance["budget_x"]
        fact_movie_performance = fact_movie_performance[[
            "financial_id", "movie_id", "date_id", "language_id", "country_id",
            "score", "budget_x", "revenue", "profit"
        ]].rename(columns={"budget_x": "budget"})
        
        return {
            "factMoviePerformance": fact_movie_performance
        }
    
    def _create_gold_tables(self, fact_table: pd.DataFrame, bridge_movie_genre: pd.DataFrame, 
                           dim_tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Create gold layer tables for business analytics.
        
        Args:
            fact_table: Fact table
            bridge_movie_genre: Movie-genre bridge table
            dim_tables: Dictionary of dimension tables
            
        Returns:
            Dictionary of gold layer tables
        """
        # Revenue by genre
        revenue_by_genre = fact_table.merge(bridge_movie_genre, on="movie_id") \
                                    .merge(dim_tables["dim_genre"], on="genre_id") \
                                    .groupby("genre_name")["revenue"].sum().reset_index() \
                                    .rename(columns={"revenue": "total_revenue"})
        
        # Average score by year
        merged_df = fact_table.merge(dim_tables["dim_movie"], on="movie_id", suffixes=('_fact', '_movie'))
        avg_score_by_year = merged_df.merge(dim_tables["dim_date"], left_on="date_id_movie", right_on="date_id", suffixes=('_movie', '_date')) \
                                    .groupby("year")["score"].mean().reset_index() \
                                    .rename(columns={"score": "avg_score"})
        
        # Add metadata
        current_time = datetime.now()
        revenue_by_genre['updated_at'] = current_time
        avg_score_by_year['updated_at'] = current_time
        
        return {
            "revenue_by_genre": revenue_by_genre,
            "avg_score_by_year": avg_score_by_year
        }