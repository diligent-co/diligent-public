package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"log"
	"strconv"
	"time"
	"net/url"
	"fmt"
	"bufio"
	"os"
	"strings"

	"coinbot/src/utils/errors"
)

func GetEnvVarsFromFile(filePath string) (map[string]string, error) {
	// open the file, read the lines, and return a map of the key-value pairs
	file, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "error opening file")
	}
	defer file.Close()

	envVars := make(map[string]string)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// split at the first =
		parts := strings.SplitN(line, "=", 2)
		envVars[parts[0]] = parts[1]
	}
	return envVars, nil
}


func createAPISign(apiSecret string) (string, string, error) {

	apiPath := "/0/private/GetWebSocketsToken"
	nonce := strconv.FormatInt(time.Now().UnixMilli(), 10)
	fixedNonce := "1634857687987"
	nonce = fixedNonce
	postData := "nonce=" + nonce
	stringToEncode := nonce + postData
	fmt.Printf("stringToEncode: %s\n", stringToEncode)

	apiSha256 := sha256.New()
	// write the concatenated nonce to the sha256 object
	apiSha256.Write([]byte(stringToEncode))
	// retrieve bytes for what's been written to the sha256 object
	apiSha256Bytes := apiSha256.Sum(nil)
	fmt.Printf("apiSha256Bytes: %v\n", apiSha256Bytes)
	// decode the api secret into bytes
	encodedSecret, err := base64.StdEncoding.DecodeString(apiSecret)
	if err != nil {
		return "", "", errors.Wrap(err, "error decoding api secret")
	}

	// create the hmac object
	h := hmac.New(sha512.New, encodedSecret)
	// write the api path and the sha256 bytes to the hmac object
	message := append([]byte(apiPath), apiSha256Bytes...)
	fmt.Printf("message: %v\n", message)
	h.Write(message)
	// retrieve the hmac digest
	hmacDigest := h.Sum(nil)
	fmt.Printf("hmacDigest: %v\n", hmacDigest)
	// encode the hmac digest into a base64 string
	apiSignature := base64.StdEncoding.EncodeToString(hmacDigest)
	fmt.Printf("apiSignature: %s\n", apiSignature)
	return postData, apiSignature, nil
}

func GetApiSignature(urlPath string, data map[string]interface{}, secret string) (string, error) {
	nonce := strconv.FormatInt(time.Now().UnixMilli(), 10)
	fixedNonce := "1634857687987"
	nonce = fixedNonce
	data["nonce"] = nonce
	
	urlValues := url.Values{}
	for key, value := range data {
		urlValues.Set(key, fmt.Sprintf("%v", value))
	}
	// convert data into json string
	urlEncodedData := urlValues.Encode()
	
	// url-encode the json string
	stringToEncode := nonce + urlEncodedData
	fmt.Printf("stringToEncode: %s\n", stringToEncode)

	sha256Hash := sha256.New()
	sha256Hash.Write([]byte(stringToEncode))
	sha256HashBytes := sha256Hash.Sum(nil)
	fmt.Printf("sha256HashBytes: %v\n", sha256HashBytes)

	encodedSecret, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return "", errors.Wrap(err, "error decoding api secret")
	}
	h := hmac.New(sha512.New, encodedSecret)
	message := append([]byte(urlPath), sha256HashBytes...)
	fmt.Printf("message: %v\n", message)
	h.Write(message)
	hmacDigest := h.Sum(nil)	
	fmt.Printf("hmacDigest: %v\n", hmacDigest)
	apiSignature := base64.StdEncoding.EncodeToString(hmacDigest)
	fmt.Printf("apiSignature: %s\n", apiSignature)
	return apiSignature, nil
}

func DecodeString(inputString string) (string, error) {
	originalInputString := inputString
	// clean the input string of any whitespace or invalid base64 characters
	inputString = strings.TrimSpace(inputString)
	inputString = strings.ReplaceAll(inputString, " ", "")
	// remove any invalid base64 characters
	inputString = strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '+' || r == '/' || r == '=' {
			return r
		}
		return -1
	}, inputString)
	fmt.Printf("cleaned inputString: %s\n", inputString)
	fmt.Printf("original == cleaned: %t\n", originalInputString == inputString)
	encodedSecret, err := base64.StdEncoding.DecodeString(inputString)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("error decoding string: %s", inputString))
	}
	return string(encodedSecret), nil
}


func main() {
	envVars, err := GetEnvVarsFromFile("../../../.env")
	if err != nil {
		log.Fatalf("Error getting env vars: %v", err)
	}
	apiSecret := envVars["KRAKEN_API_SECRET"]

	data := map[string]interface{}{}
	//postData, apiSignature, err := createAPISign(apiSecret)
	apiSignature, err := GetApiSignature("/0/private/GetWebSocketsToken", data, apiSecret)
	if err != nil {
		log.Fatalf("Error creating API sign: %v", err)
	}
	_, apiSig2, err := createAPISign(apiSecret)
	if err != nil {
		log.Fatalf("Error creating API sign: %v", err)
	}
	// check if they are the same
	fmt.Printf("apiSig1: %s\n", apiSignature)
	fmt.Printf("apiSig2: %s\n", apiSig2)
}