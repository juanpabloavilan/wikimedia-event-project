package main

import (
	"crypto/tls"
	"net/http"

	opensearch "github.com/opensearch-project/opensearch-go"
)

func main() {
	client, err := opensearch.NewClient(opensearch.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses: []string{"http://localhost:9200"},
	})

}
