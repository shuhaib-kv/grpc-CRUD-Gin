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
	"google.golang.org/grpc"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func init() {
	DatabaseConnection()
}

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
	// viper.SetConfigName(".env")
	// viper.AddConfigPath("../")
	// err := viper.ReadInConfig()
	// if err != nil {
	// 	panic(fmt.Errorf("Fatal error config file: %s", err))
	// }
	// host := viper.GetString("POSTGRES_HOST")
	// port := viper.GetInt("POSTGRES_PORT")
	// dbUser := viper.GetString("POSTGRES_USER")
	// password := viper.GetString("POSTGRES_PASSWORD")
	// dbName := viper.GetString("POSTGRES_DB")
	host := "localhost"
	port := "5432"
	dbUser := "shuhaib"
	password := "soib"
	dbName := "postgres"

	fmt.Println(host)

	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=disable", host, port, dbUser, dbName, password)
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

func (*server) GetMovie(ctx context.Context, req *moviepb.ReadMovieRequest) (*moviepb.ReadMovieResponse, error) {
	fmt.Println("View Movie")
	Movieid := req.GetId()
	var data *Movie
	res := DB.Table("movies").Select("title", "genre").Where("id=?", Movieid).Find(&data)

	if res.RowsAffected == 0 {
		return nil, errors.New("movie creation unsuccessful")

	}
	return &moviepb.ReadMovieResponse{

		Movie: &moviepb.Movie{
			Id:    data.ID,
			Title: data.Title,
			Genre: data.Genre,
		},
	}, nil

}
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

func (*server) GetMovies(ctx context.Context, req *moviepb.ReadMoviesRequest) (*moviepb.ReadMoviesResponse, error) {
	fmt.Println("View All Movie")
	movies := []*moviepb.Movie{}
	res := DB.Table("movies").Select("id", "title", "genre").Scan(&movies)
	if res.RowsAffected == 0 {
		return nil, errors.New("movie creation unsuccessful")
	}

	return &moviepb.ReadMoviesResponse{
		Movies: movies,
	}, nil
}

func (*server) UpdateMovie(ctx context.Context, req *moviepb.UpdateMovieRequest) (*moviepb.UpdateMovieResponse, error) {
	reqMovie := req.GetMovie()
	fmt.Println(reqMovie)
	res := DB.Model("").Select("id", "title", "genre").Where("id=?", reqMovie.Id).Updates(Movie{ID: reqMovie.Id, Genre: reqMovie.Genre, Title: reqMovie.Title})
	if res.RowsAffected == 0 {
		return nil, errors.New("movie updation unsuccessful")
	}
	return &moviepb.UpdateMovieResponse{
		Movie: reqMovie,
	}, nil
}
func (*server) DeleteMovie(ctx context.Context, req *moviepb.DeleteMovieRequest) (*moviepb.DeleteMovieResponse, error) {
	fmt.Println("Delete Movie")
	movieid := req.GetId()
	var movie Movie
	res := DB.First(&movie, "id=?", movieid)

	if res.RowsAffected == 0 {
		return nil, errors.New("Couldnt find movie")
	}
	DB.Delete(&Movie{}, "id=?", movieid)
	return &moviepb.DeleteMovieResponse{
		Success: true,
	}, nil
}

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
