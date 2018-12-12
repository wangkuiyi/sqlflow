package sqlfs

import (
	"database/sql"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

var (
	testCfg *mysql.Config
	testDB  *sql.DB
)

func TestCreateHasAndDropTable(t *testing.T) {
	a := assert.New(t)

	fn := fmt.Sprintf("sqlfs.unitest%d", rand.Int())
	a.NoError(createTable(testDB, fn))
	has, e := HasTable(testDB, fn)
	a.NoError(e)
	a.True(has)
	a.NoError(DropTable(testDB, fn))
}

func TestWriterCreate(t *testing.T) {
	a := assert.New(t)

	fn := fmt.Sprintf("sqlfs.unitest%d", rand.Int())
	w, e := Create(testDB, fn)
	a.NoError(e)
	a.NotNil(w)
	defer w.Close()

	has, e1 := HasTable(testDB, fn)
	a.NoError(e1)
	a.True(has)

	a.NoError(DropTable(testDB, fn))
}

func TestWriteAndRead(t *testing.T) {
	a := assert.New(t)

	fn := fmt.Sprintf("sqlfs.unitest%d", rand.Int())

	w, e := Create(testDB, fn)
	a.NoError(e)
	a.NotNil(w)

	// A small output.
	buf := []byte("\000\000\000")
	n, e := w.Write(buf)
	a.NoError(e)
	a.Equal(len(buf), n)

	// A big output.
	buf = make([]byte, kBufSize+1)
	for i := range buf {
		buf[i] = 'x'
	}
	n, e = w.Write(buf)
	a.NoError(e)
	a.Equal(len(buf), n)

	a.NoError(w.Close())

	r, e := Open(testDB, fn)
	a.NoError(e)
	a.NotNil(r)

	// A small read
	buf = make([]byte, 2)
	n, e = r.Read(buf)
	a.NoError(e)
	a.Equal(2, n)
	a.Equal(2, strings.Count(string(buf), "\000"))

	// A big read of rest
	buf = make([]byte, kBufSize*2)
	n, e = r.Read(buf)
	a.Equal(io.EOF, e)
	a.Equal(kBufSize+2, n)
	a.Equal(1, strings.Count(string(buf[:n]), "\000"))
	a.Equal(kBufSize+1, strings.Count(string(buf[:n]), "x"))

	// Another big read
	n, e = r.Read(buf)
	a.Equal(io.EOF, e)
	a.Equal(0, n)
	a.NoError(r.Close())

	a.NoError(DropTable(testDB, fn))
}

func TestRepeatedWriteAndRead(t *testing.T) {
	a := assert.New(t)
	fn := fmt.Sprintf("sqlfs.unitest%d", rand.Int())
	{
		w, e := Create(testDB, fn)
		a.NoError(e)
		a.NotNil(w)

		buf := []byte("\n\000\n\000")
		for i := 0; i < 10; i++ {
			n, e := w.Write(buf)
			a.NoError(e)
			a.Equal(len(buf), n)
		}
		a.NoError(w.Close())
	}
	{
		r, e := Open(testDB, fn)
		a.NoError(e)
		a.NotNil(r)

		buf := make([]byte, 4)
		for i := 0; i < 10; i++ {
			n, e := r.Read(buf)
			a.NoError(e)
			a.Equal(4, n)
			a.Equal(2, strings.Count(string(buf), "\000"))
			a.Equal(2, strings.Count(string(buf), "\n"))
		}
		a.NoError(r.Close())
	}
	a.NoError(DropTable(testDB, fn))
}

func TestRepeatedWriteAndReadGob(t *testing.T) {
	hello := "Hello World!\n"
	a := assert.New(t)
	fn := fmt.Sprintf("sqlfs.unitest%d", rand.Int())
	{
		w, e := Create(testDB, fn)
		a.NoError(e)
		a.NotNil(w)
		for i := 0; i < 10; i++ {
			a.NoError(gob.NewEncoder(w).Encode(hello))
		}
		a.NoError(w.Close())
	}
	{
		r, e := Open(testDB, fn)
		a.NoError(e)
		a.NotNil(r)
		var buf string
		dec := gob.NewDecoder(r)
		for i := 0; i < 10; i++ {
			a.NoError(dec.Decode(&buf))
			a.Equal(hello, buf)
		}
		a.Equal(io.EOF, dec.Decode(&buf))
		a.NoError(r.Close())
	}
	a.NoError(DropTable(testDB, fn))
}

func TestReadReturnEOF(t *testing.T) {
	a := assert.New(t)
	fn := fmt.Sprintf("sqlfs.unitest%d", rand.Int())
	{
		w, e := Create(testDB, fn)
		a.NoError(e)
		a.NotNil(w)

		// A small output.
		buf := []byte("\000\n\000")
		n, e := w.Write(buf)
		a.NoError(e)
		a.Equal(len(buf), n)

		a.NoError(w.Close())
	}
	{
		r, e := Open(testDB, fn)
		a.NoError(e)
		a.NotNil(r)

		// Read exactly 3 bytes shouldn't return EOF.
		buf := make([]byte, 3)
		n, e := r.Read(buf)
		a.NoError(e)
		a.Equal(3, n)
		a.Equal(2, strings.Count(string(buf), "\000"))
		a.Equal(1, strings.Count(string(buf), "\n"))

		// Reading more should return EOF
		n, e = r.Read(buf)
		a.Equal(io.EOF, e)
		a.Equal(0, n)

		a.NoError(r.Close())
	}
	a.NoError(DropTable(testDB, fn))
}

func TestWriteAndReadGob(t *testing.T) {
	a := assert.New(t)
	fn := fmt.Sprintf("sqlfs.unitest%d", rand.Int())
	{
		w, e := Create(testDB, fn)
		a.NoError(e)
		a.NotNil(w)
		w.Write([]byte("\n\n\000"))
		a.NoError(gob.NewEncoder(w).Encode("Hello World!\n"))
		w.Write([]byte("\n\n\000"))
		a.NoError(w.Close())
	}
	{
		r, e := Open(testDB, fn)
		a.NoError(e)
		a.NotNil(r)

		buf := make([]byte, 3)
		n, e := r.Read(buf)
		a.Equal(3, n)
		a.NoError(e)

		var hello string
		a.NoError(gob.NewDecoder(r).Decode(&hello))
		a.Equal("Hello World!\n", hello)

		// gob.Decoder is a buffered reader and tends to read
		// what's left into its buffer.
		n, e = r.Read(buf)
		a.Equal(0, n)
		a.Equal(io.EOF, e)

		a.NoError(r.Close())
	}
	a.NoError(DropTable(testDB, fn))
}

func TestMain(m *testing.M) {
	testCfg = &mysql.Config{
		User:   "root",
		Passwd: "root",
		Addr:   "localhost:3306",
	}
	db, e := sql.Open("mysql", testCfg.FormatDSN())
	if e != nil {
		log.Panicf("TestMain cannot connect to MySQL: %q.\n"+
			"Please run MySQL server as in example/churn/README.md.", e)
	}
	testDB = db

	defer testDB.Close()
	os.Exit(m.Run())
}
