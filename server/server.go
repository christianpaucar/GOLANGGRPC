package main

import (
	"context"
	"fmt"
	"generate/products/productpb"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var collection *mongo.Collection

type product struct {
	ID    primitive.ObjectID `bson:"_id,omitempty"`
	Name  string             `bson:"name"`
	Price float64            `bson:"price"`
}

type server struct{}

//insert one Porduct
func (*server) CreateProduct(ctx context.Context, req *productpb.CreateProductRequest) (*productpb.CreateProductResponse, error) {
	//parse
	prod := req.GetProduct()
	data := product{
		Name:  prod.GetName(),
		Price: prod.GetPrice(),
	}
	res, err := collection.InsertOne(context.Background(), data)

	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("internal error %v", err),
		)
	}
	oid, ok := res.InsertedID.(primitive.ObjectID)

	if !ok {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("commont convert OID : %v", err),
		)
	}

	return &productpb.CreateProductResponse{
		Product: &productpb.Product{
			Id:    oid.Hex(),
			Name:  prod.GetName(),
			Price: prod.GetPrice(),
		},
	}, nil

}

//Search a product
func (*server) GetProduct(ctx context.Context, req *productpb.GetProductRequest) (*productpb.GetProductResponse, error) {
	productId := req.GetProductId()
	oid, err := primitive.ObjectIDFromHex(productId)

	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("cannot parser ID:"),
		)
	}

	//estructura
	data := &product{}

	filter := bson.M{"_id": oid}
	res := collection.FindOne(context.Background(), filter)

	if err := res.Decode(data); err != nil {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintf("cannot fin the product:%v", err),
		)
	}

	return &productpb.GetProductResponse{
		Product: dbToProductPb(data),
	}, nil

}

//update product by id for the server
func (*server) UpdateProduct(ctx context.Context, req *productpb.UpdateProductRequest) (*productpb.UpdateProductResponse, error) {
	fmt.Println("update product request")

	prod := req.GetProduct()
	oid, err := primitive.ObjectIDFromHex(prod.GetId())

	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("cannot parser ID:"),
		)
	}

	//create empty struc
	data := &product{}
	filter := bson.M{"_id": oid}
	//update empty struc
	res := collection.FindOne(context.Background(), filter)
	if res.Decode(data); err != nil {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintf("cannot fin the product with the id %v", err),
		)
	}

	//update the internal struct product
	data.Name = prod.GetName()
	data.Price = prod.GetPrice()

	//update en data base
	_, updateErr := collection.ReplaceOne(context.Background(), filter, data)
	if updateErr != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("cannot update de  product in data base %v", updateErr),
		)
	}
	return &productpb.UpdateProductResponse{
		Product: dbToProductPb(data),
	}, nil
}

// list products for the server
func (*server) ListProduct(req *productpb.ListProductRequest, stream productpb.ProductService_ListProductServer) error {
	fmt.Println("list product request")
	cur, err := collection.Find(context.Background(), primitive.D{{}})
	if err != nil {
		return status.Errorf(
			codes.Internal,
			fmt.Sprintf("internal error %v", err),
		)
	}

	defer cur.Close(context.Background())

	for cur.Next(context.Background()) {
		data := &product{}
		err := cur.Decode(data)

		if err != nil {
			return status.Errorf(
				codes.Internal,
				fmt.Sprintf("error decoding data %v", err),
			)
		}

		stream.Send(&productpb.ListProductResponse{
			Product: dbToProductPb(data),
		})

	}

	if err := cur.Err(); err != nil {
		return status.Errorf(
			codes.Internal,
			fmt.Sprintf("error decoding data %v", err),
		)
	}
	return nil

}

//delete product by id for the server
func (*server) DeleteProduct(ctx context.Context, req *productpb.DeleteProductRequest) (*productpb.DeleteProductResponse, error) {
	fmt.Println("delete product request")

	oid, err := primitive.ObjectIDFromHex(req.GetProductId())

	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("cannot parser ID:"),
		)
	}

	filter := bson.M{"_id": oid}
	res, err := collection.DeleteOne(context.Background(), filter)

	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("cannot delet product %v", err),
		)
	}

	if res.DeletedCount == 0 {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintf("cannot find the product %v", err),
		)
	}

	return &productpb.DeleteProductResponse{
		ProductId: req.GetProductId(),
	}, nil
}

//Function transform data
func dbToProductPb(data *product) *productpb.Product {
	return &productpb.Product{
		Id:    data.ID.Hex(),
		Name:  data.Name,
		Price: data.Price,
	}

}

func main() {

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	fmt.Println("CONECTING TO MONGO DB")
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalf("error creando el cliente DB %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatalf("error al conectar con DB %v", err)
	}

	collection = client.Database("productsdb").Collection("products")

	fmt.Println("servidor de productos esta corriendo GRPC")
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("failed to listen %v", err)

	}

	s := grpc.NewServer()
	productpb.RegisterProductServiceServer(s, &server{})

	go func() {
		fmt.Println("iniciando el servidor")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("el servidor a fallado %v", err)
		}
	}()

	//wait for ctrl + x to exit

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	<-ch
	fmt.Println("stop the server")
	s.Stop()
	fmt.Println("clossing the listenner")
	lis.Close()
	fmt.Println("cerrando la conexion con la  base de  datos")
	client.Disconnect(context.TODO())
	fmt.Println("close finished...")

}
