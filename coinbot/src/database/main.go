package database

import (
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"strconv"

	slogGorm "github.com/orandin/slog-gorm"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"coinbot/src/datamodels"
	"coinbot/src/utils/errors"
)

type CoinbotDatabase interface {
	TransactionDb
	AggFeedDatabase
	MetricsDatabase
}

type databaseImplementation struct {
	gormDb              *gorm.DB
	notificationManager *NotificationManager
}

func NewDBConnection(dbConfig datamodels.PostgresConfig) (CoinbotDatabase, error) {
	var err error

	dbConnString := MakeConnectionString(&dbConfig)

	gormConfig := &gorm.Config{
		Logger: slogGorm.New(),
	}

	gorm, err := gorm.Open(postgres.Open(dbConnString), gormConfig)
	if err != nil {
		return nil, errors.WrapE(err, errors.New("cannot create gorm engine"))
	}

	slog.Info("Connected to database", "host", dbConfig.Host, "database", dbConfig.Database, "user", dbConfig.User)

	notifyManager, err := NewNotificationManager(gorm)
	if err != nil {
		return nil, errors.WrapE(err, errors.New("cannot create notify manager"))
	}

	centariDB := &databaseImplementation{
		gormDb:              gorm,
		notificationManager: notifyManager,
	}

	return centariDB, nil
}

func MakeConnectionString(dbConfig *datamodels.PostgresConfig) string {
	if dbConfig.URI != "" { // If url is provided, use it
		return dbConfig.URI
	}

	ssl := "sslmode=" + dbConfig.SSL.Mode

	if dbConfig.SSL.Mode != "disable" {
		sslFiles := map[string]string{
			"sslcert":     dbConfig.SSL.Cert,
			"sslkey":      dbConfig.SSL.Key,
			"sslrootcert": dbConfig.SSL.CA,
		}

		for param, content := range sslFiles {
			if content != "" {
				file, err := writeCertificate(content, param+".pem")
				if err != nil {
					slog.Error("Error writing " + param + " to file: " + err.Error())
				}

				ssl += "&" + param + "=" + file
			}
		}
	}

	hostPort := net.JoinHostPort(dbConfig.Host, strconv.Itoa(dbConfig.Port))

	if dbConfig.Password == "" {
		slog.Warn("No password provided for database connection, using empty password")
		return fmt.Sprintf("postgres://%s@%s/%s?search_path=public&%s",
			dbConfig.User,
			hostPort,
			dbConfig.Database,
			ssl,
		)
	}

	return fmt.Sprintf("postgres://%s:%s@%s/%s?search_path=public&%s",
		dbConfig.User,
		dbConfig.Password,
		hostPort,
		dbConfig.Database,
		ssl,
	)
}

func writeCertificate(content string, outFile string) (string, error) {
	tempFile, err := os.CreateTemp("", outFile)
	if err != nil {
		return "", err
	}

	_, err = tempFile.WriteString(content)
	if err != nil {
		tempFile.Close()

		return "", err
	}

	err = tempFile.Close()
	if err != nil {
		log.Printf("Error closing %s: %v\n", outFile, err)
	}

	return tempFile.Name(), nil
}
