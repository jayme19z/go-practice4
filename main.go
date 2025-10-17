package main

import (
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type User struct {
	ID      int     `db:"id"`
	Name    string  `db:"name"`
	Email   string  `db:"email"`
	Balance float64 `db:"balance"`
}

func main() {
	dsn := "user=user password=password dbname=mydatabase sslmode=disable host=localhost port=5430"
	db, err := sqlx.Open("postgres", dsn)

	if err != nil {
		log.Fatalln("Error connecting to database:", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatalln("Could not ping to the database:", err)
	}

	fmt.Println("Connected to the database successfully!")

	user1 := User{Name: "Zhamilya", Email: "z_kozhagulova@kbtu.kz", Balance: 5000.0}
	err = InsertUser(db, user1)
	if err != nil {
		log.Println("Insert user1 error:", err)
	}

	user2 := User{Name: "Assel", Email: "a_derbisova@nu.edu.kz", Balance: 3500.0}
	err = InsertUser(db, user2)
	if err != nil {
		log.Println("Insert user2 error:", err)
	}

	users, _ := GetAllUsers(db)
	fmt.Println("All users:", users)

	err = TransferBalance(db, 1, 2, 500)
	if err != nil {
		log.Println("Transfer failed:", err)
	} else {
		fmt.Println("Transfer succeeded!")
	}
}

func InsertUser(db *sqlx.DB, user User) error {
	query := `INSERT INTO users (name, email, balance) VALUES (:name, :email, :balance)`
	_, err := db.NamedExec(query, user)
	return err
}

func GetAllUsers(db *sqlx.DB) ([]User, error) {
	var users []User
	err := db.Select(&users, "SELECT * FROM users")
	return users, err
}

func GetUserByID(db *sqlx.DB, id int) (User, error) {
	var user User
	err := db.Get(&user, "SELECT * FROM users WHERE id=$1", id)
	return user, err
}

func TransferBalance(db *sqlx.DB, fromID int, toID int, amount float64) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	var from User
	if err := tx.Get(&from, "SELECT * FROM users WHERE id=$1 FOR UPDATE", fromID); err != nil {
		return fmt.Errorf("Could not find the sender: %v", err)
	}

	var to User
	if err := tx.Get(&to, "SELECT * FROM users WHERE id=$1 FOR UPDATE", toID); err != nil {
		return fmt.Errorf("Could not find the receiver: %v", err)
	}

	if from.Balance < amount {
		return fmt.Errorf("Insufficient balance")
	}

	_, err = tx.Exec("UPDATE users SET balance = balance - $1 WHERE id = $2", amount, fromID)
	if err != nil {
		return err
	}

	_, err = tx.Exec("UPDATE users SET balance = balance + $1 WHERE id = $2", amount, toID)
	if err != nil {
		return err
	}

	return nil
}
