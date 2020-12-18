package main

import (
	"context"
	"fmt"
	"generate/products/productpb"
	"io"
	"log"

	"google.golang.org/grpc"
)

func main() {
	fmt.Println("el cliente esta corriendo")

	opts := grpc.WithInsecure()

	cc, err := grpc.Dial("localhost:50051", opts)

	if err != nil {
		log.Fatalf("fallo el cliente %v", err)
	}

	defer cc.Close()

	c := productpb.NewProductServiceClient(cc)
	//creating product
	fmt.Println("------------------creating product------------------")
	product := &productpb.Product{
		Name:  "smartphone",
		Price: 15.99,
	}

	createdProduct, err := c.CreateProduct(context.Background(), &productpb.CreateProductRequest{
		Product: product,
	})

	if err != nil {
		log.Fatalf("fallo la creacion de producto %v", err)
	}
	//created the product print
	fmt.Printf("Product Created %v ", createdProduct)

	fmt.Println("")
	//Get Product  for id
	fmt.Println("-----------------Geting Product---------------------")

	productID := createdProduct.GetProduct().GetId()

	getProductReq := &productpb.GetProductRequest{ProductId: productID}

	getProductRes, getProductErr := c.GetProduct(context.Background(), getProductReq)

	if getProductErr != nil {
		log.Fatalf("fallo la obtencion de producto %v", getProductErr)
	}
	// get product print
	fmt.Printf("product gooten: %v", getProductRes)

	fmt.Println("-----------------Update Product---------------------")

	newProduct := &productpb.Product{
		Id:    productID,
		Name:  "new name:smartphone",
		Price: 5055.25,
	}

	updateRes, updateErr := c.UpdateProduct(context.Background(), &productpb.UpdateProductRequest{
		Product: newProduct,
	})

	if updateErr != nil {
		log.Fatalf("error happened while updating %v", updateErr)
	}

	fmt.Printf("product Updated %v", updateRes)

	fmt.Println("-----------------Delete Product---------------------")

	deleteRes, deleteErr := c.DeleteProduct(context.Background(), &productpb.DeleteProductRequest{
		//ProductId: productID,
		ProductId: "5fd525ec0db276ae26ada802",
	})

	if deleteErr != nil {
		fmt.Printf("Error deleting the product %v", deleteErr)
	}

	fmt.Printf("product deleted:%v", deleteRes.GetProductId())

	fmt.Println("-----------------List   Product---------------------")
	stream, err := c.ListProduct(context.Background(), &productpb.ListProductRequest{})

	if err != nil {
		log.Fatalf("error callin list product %v", err)
	}

	for {
		res, err := stream.Recv()

		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("error reciving product %v", err)
		}

		fmt.Println(res.GetProduct())
	}
}
