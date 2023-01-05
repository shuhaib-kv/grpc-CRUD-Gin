package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/shuhaib-kv/proto/moviepb"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func init() {
	DatabaseConnection()
}

// type config struct{

//		Port string `mapstructure:"PORT"`
//	}
var DB *gorm.DB
var err error

type Movie struct {
	ID        string `gorm:"primarykey"`
	Title     string
	Genre     string
	CreatedAt time.Time `gorm:"autoCreateTime:false"`
	UpdatedAt time.Time `gorm:"autoUpdateTime:false"`
}

func DatabaseConnection() {
	viper.SetConfigName("app")
	viper.AddConfigPath("./../.")
	viper.SetConfigType("env")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s", err))
	}
	host := viper.GetString("DATABASE_HOST")
	port := viper.GetInt("DATABASE_PORT")
	dbUser := viper.GetString("DATABASE_USER")
	password := viper.GetString("DATABASE_PASSWORD")
	dbName := viper.GetString("POSTGRES_DB")
	// fmt.Println(host)
	// host := "localhost"
	// port := "5432"
	// dbUser := "shuhaib"
	// password := "soib"
	// dbName := "postgres"

	fmt.Println(host)

	dsn := fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%s sslmode=disable", host, port, dbUser, dbName, password)
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	DB.AutoMigrate(Movie{})
	if err != nil {
		log.Fatal("Error connecting to the database...", err)
	}
	fmt.Println("Database connection successful...")
}

var (
	port = flag.Int("port", 50051, "gRPC server port")
)

type server struct {
	moviepb.UnimplementedMovieServiceServer
}

// func (*server) GetMovie(ctx context.Context, req *moviepb.ReadMovieRequest) (*moviepb.ReadMovieResponse, error) {
// 	fmt.Println("View Movie")
// 	Movieid := req.GetId()
// 	var data *Movie
// 	res := DB.Table("movies").Select("title", "genre").Where("id=?", Movieid).Find(&data)

// 	if res.RowsAffected == 0 {
// 		return nil, errors.New("movie creation unsuccessful")

// 	}
// 	return &moviepb.ReadMovieResponse{

// 		Movie: &moviepb.Movie{
// 			Id:    data.ID,
// 			Title: data.Title,
// 			Genre: data.Genre,
// 		},
// 	}, nil

// }
func (*server) CreateMovie(ctx context.Context, req *moviepb.CreateMovieRequest) (*moviepb.CreateMovieResponse, error) {
	fmt.Println("Create Movie")
	movie := req.GetMovie()
	movie.Id = uuid.New().String()

	data := Movie{
		ID:    movie.GetId(),
		Title: movie.GetTitle(),
		Genre: movie.GetGenre(),
	}

	res := DB.Create(&data)
	if res.RowsAffected == 0 {
		return nil, errors.New("movie creation unsuccessful")
	}
	return &moviepb.CreateMovieResponse{
		Movie: &moviepb.Movie{
			Id:    movie.GetId(),
			Title: movie.GetTitle(),
			Genre: movie.GetGenre(),
		},
	}, nil
}
func (*server) GetMovie(ctx context.Context, req *moviepb.ReadMovieRequest) (*moviepb.ReadMovieResponse, error) {
	fmt.Println("Read Movie", req.GetId())
	var movie Movie
	res := DB.Find(&movie, "id = ?", req.GetId())
	if res.RowsAffected == 0 {
		return nil, errors.New("movie not found")
	}
	return &moviepb.ReadMovieResponse{
		Movie: &moviepb.Movie{
			Id:    movie.ID,
			Title: movie.Title,
			Genre: movie.Genre,
		},
	}, nil
}

func (*server) GetMovies(ctx context.Context, req *moviepb.ReadMoviesRequest) (*moviepb.ReadMoviesResponse, error) {
	fmt.Println("Read Movies")
	movies := []*moviepb.Movie{}
	res := DB.Find(&movies)
	if res.RowsAffected == 0 {
		return nil, errors.New("movie not found")
	}
	return &moviepb.ReadMoviesResponse{
		Movies: movies,
	}, nil
}

func (*server) UpdateMovie(ctx context.Context, req *moviepb.UpdateMovieRequest) (*moviepb.UpdateMovieResponse, error) {
	fmt.Println("Update Movie")
	var movie Movie
	reqMovie := req.GetMovie()

	res := DB.Model(&movie).Where("id=?", reqMovie.Id).Updates(Movie{Title: reqMovie.Title, Genre: reqMovie.Genre})

	if res.RowsAffected == 0 {
		return nil, errors.New("movies not found")
	}

	return &moviepb.UpdateMovieResponse{
		Movie: &moviepb.Movie{
			Id:    movie.ID,
			Title: movie.Title,
			Genre: movie.Genre,
		},
	}, nil
}

func (*server) DeleteMovie(ctx context.Context, req *moviepb.DeleteMovieRequest) (*moviepb.DeleteMovieResponse, error) {
	fmt.Println("Delete Movie")
	var movie Movie
	res := DB.Where("id = ?", req.GetId()).Delete(&movie)
	if res.RowsAffected == 0 {
		return nil, errors.New("movie not found")
	}

	return &moviepb.DeleteMovieResponse{
		Success: true,
	}, nil
}

// func (*server) GetMovies(ctx context.Context, req *moviepb.ReadMoviesRequest) (*moviepb.ReadMoviesResponse, error) {
// 	fmt.Println("View All Movie")
// 	movies := []*moviepb.Movie{}
// 	res := DB.Table("movies").Select("id", "title", "genre").Scan(&movies)
// 	if res.RowsAffected == 0 {
// 		return nil, errors.New("movie creation unsuccessful")
// 	}

// 	return &moviepb.ReadMoviesResponse{
// 		Movies: movies,
// 	}, nil
// }

// func (*server) UpdateMovie(ctx context.Context, req *moviepb.UpdateMovieRequest) (*moviepb.UpdateMovieResponse, error) {
// 	reqMovie := req.GetMovie()
// 	fmt.Println(reqMovie)
// 	res := DB.Model("").Select("id", "title", "genre").Where("id=?", reqMovie.Id).Updates(Movie{ID: reqMovie.Id, Genre: reqMovie.Genre, Title: reqMovie.Title})
// 	if res.RowsAffected == 0 {
// 		return nil, errors.New("movie updation unsuccessful")
// 	}
// 	return &moviepb.UpdateMovieResponse{
// 		Movie: reqMovie,
// 	}, nil
// }
// func (*server) DeleteMovie(ctx context.Context, req *moviepb.DeleteMovieRequest) (*moviepb.DeleteMovieResponse, error) {
// 	fmt.Println("Delete Movie")
// 	movieid := req.GetId()
// 	var movie Movie
// 	res := DB.First(&movie, "id=?", movieid)

// 	if res.RowsAffected == 0 {
// 		return nil, errors.New("Couldnt find movie")
// 	}
// 	DB.Delete(&Movie{}, "id=?", movieid)
// 	return &moviepb.DeleteMovieResponse{
// 		Success: true,
// 	}, nil
// }

func main() {
	fmt.Println("gRPC server running ...")

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()

	moviepb.RegisterMovieServiceServer(s, &server{})

	log.Printf("Server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve : %v", err)
	}
}
