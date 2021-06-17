package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"unsafe"
)

type statusCode int

const (
	META_COMMAND_SUCCESS statusCode = iota
	META_COMMAND_UNRECOGNIZED
	PREPARE_SUCCESS
	PREPARE_SYNTAX_ERROR
	PREPARE_UNRECOGNIZED_STATEMENT
	EXECUTE_SUCCESS
	EXECUTE_TABLE_FULL
)

type statementType int

const (
	STATEMENT_INSERT statementType = iota
	STATEMENT_SELECT
)

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
	PAGE_SIZE            = 4096
	TABLE_MAX_PAGE       = 100
)

var row_ *row = NewRow()

type pager struct {
	fd         *os.File
	fileLength uint32
	pages      []*page
}

func NewPager(filename string) *pager {
	file, err := os.OpenFile("filename", os.O_RDWR|os.O_CREATE, syscall.S_IWUSR|syscall.S_IRUSR)

	if err != nil {
		fmt.Println("Unable to open file \n")
		os.Exit(1)
	}

	fs, err := file.Stat()
	if err != nil {
		fmt.Println("Unable to fetch stat")
	}

	p := &pager{}
	p.fileLength = uint32(fs.Size())
	p.fd = file
	p.pages = make([]*page, TABLE_MAX_PAGE)

	return p
}

func (p *pager) getPage(pageNum uint32) *page {
	if pageNum > TABLE_MAX_PAGE {
		fmt.Println("Tried to fetch page number out of bound", TABLE_MAX_PAGE)
		os.Exit(1)
	}
	var numOfPages uint32
	if p.pages[pageNum] == nil {
		page := NewPage()
		numOfPages = p.fileLength / PAGE_SIZE

		if p.fileLength%PAGE_SIZE > 0 {
			numOfPages++
		}

		if pageNum <= numOfPages {
			o, _ := p.fd.Seek(int64(pageNum*PAGE_SIZE), io.SeekStart)
			log("seeked to offset", o)
			_, e := p.fd.Read(page.bytes)
			if e != nil {
				log("Error while reading bytes from file")
			}
			p.pages[pageNum] = page
		}
	}

	return p.pages[pageNum]
}

func (p *pager) flush(pageNum uint32, size uint32) {
	if p.pages[pageNum] == nil {
		log("Tried to flush null")
		os.Exit(1)
	}

	_, err := p.fd.Seek(int64(pageNum*PAGE_SIZE), io.SeekStart)
	if err != nil {
		log("Error seeking file")
		os.Exit(1)
	}

	_, err = p.fd.Write(p.pages[pageNum].bytes[:size])
	if err != nil {
		log("Error writing")
		os.Exit(1)
	}
}

type page struct {
	bytes []byte
}

func NewPage() *page {
	p := &page{
		bytes: make([]byte, PAGE_SIZE),
	}
	return p
}

type table struct {
	numOfRows uint32
	pager     *pager
}

func openDB(filename string) *table {
	t := &table{}
	t.pager = NewPager(filename)
	t.numOfRows = t.pager.fileLength / row_.size
	return t
}

func (t *table) closeDB() {

	numOfFullPages := t.numOfRows / row_.rowsPerPage

	for i := 0; i < int(numOfFullPages); i++ {
		if t.pager.pages[i] == nil {
			continue
		}
		t.pager.flush(uint32(i), PAGE_SIZE)
		t.pager.pages[i] = nil
	}

	//there may be a partial page to write to the end of the file
	//this should not be needed after we switch to B-Tree
	additionalRows := t.numOfRows % row_.rowsPerPage
	if additionalRows > 0 {
		pageNum := numOfFullPages
		if t.pager.pages[pageNum] != nil {
			t.pager.flush(pageNum, additionalRows*row_.size)
			t.pager.pages[pageNum] = nil
		}
	}

	err := t.pager.fd.Close()
	if err != nil {
		log("Error while closing fd")
		os.Exit(1)
	}
}

func (t *table) rowSlot(rowNum uint32) (*page, uint32) {
	pageNum := rowNum / row_.rowsPerPage
	log("pageNum", pageNum)
	page := t.pager.getPage(rowNum)

	rowOffset := rowNum % row_.rowsPerPage
	byteOffset := rowOffset * row_.size
	log("byteOffset", byteOffset)
	return page, byteOffset
}

func (t *table) deserializeRow(rowNum uint32) *row {
	row := NewRow()
	page, offset := t.rowSlot(rowNum)
	offset = offset + row.idOffset
	row.id = binary.LittleEndian.Uint32(page.bytes[offset : offset+row.idSize])
	offset = offset + row.usernameOffset
	row.username = page.bytes[offset : offset+row.usernameSize]
	offset = offset + row.emailOffset
	row.email = page.bytes[offset : offset+row.emailSize]

	return row
}

type row struct {
	id             uint32
	username       []byte
	email          []byte
	idOffset       uint32
	idSize         uint32
	usernameOffset uint32
	usernameSize   uint32
	emailOffset    uint32
	emailSize      uint32
	size           uint32
	rowsPerPage    uint32
	tableMaxRows   uint32
}

func NewRow() *row {
	_row := &row{
		username: make([]byte, COLUMN_USERNAME_SIZE),
		email:    make([]byte, COLUMN_EMAIL_SIZE),
	}
	_row.idSize = uint32(unsafe.Sizeof(_row.id))
	_row.usernameSize = uint32(unsafe.Sizeof(_row.username))
	_row.emailSize = uint32(unsafe.Sizeof(_row.email))

	_row.idOffset = 0
	_row.usernameOffset = _row.idOffset + _row.idSize
	_row.emailOffset = _row.usernameOffset + _row.usernameSize
	_row.size = _row.emailOffset + _row.emailSize

	_row.rowsPerPage = PAGE_SIZE / _row.size
	_row.tableMaxRows = _row.rowsPerPage * TABLE_MAX_PAGE
	return _row
}

func (r *row) serialize(t *table) {
	p, offset := t.rowSlot(t.numOfRows)
	offset = offset + r.idOffset
	binary.LittleEndian.PutUint32(p.bytes[offset:offset+r.idSize], r.id)
	offset = offset + r.usernameOffset
	copy(p.bytes[offset:offset+r.usernameSize], r.username)
	offset = offset + r.emailOffset
	copy(p.bytes[offset:offset+r.emailSize], r.email)
}

func (r *row) print() {
	fmt.Println("( ", r.id, string(r.username), string(r.email), " )")
}

type statement struct {
	sType       statementType
	rowToInsert *row
}

func NewStatement() *statement {
	s := &statement{
		rowToInsert: NewRow(),
	}

	return s
}

func (s *statement) prepareStatement(input string) statusCode {
	if strings.Compare(input[:6], "insert") == 0 {
		s.sType = STATEMENT_INSERT
		//s.rowToInsert = NewRow()

		inputarray := strings.Split(input, " ")
		if len(inputarray) < 4 {
			return PREPARE_SYNTAX_ERROR
		}
		id, e := strconv.Atoi(inputarray[1])
		if e != nil {
			fmt.Println("Invalid id")
			return PREPARE_SYNTAX_ERROR
		}
		s.rowToInsert.id = uint32(id)
		copy(s.rowToInsert.username, inputarray[2])
		copy(s.rowToInsert.email, inputarray[3])

		return PREPARE_SUCCESS
	}

	if strings.Compare(input, "select") == 0 {
		s.sType = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}

	return PREPARE_UNRECOGNIZED_STATEMENT
}

func (s *statement) execute(t *table) statusCode {
	log("Statement.execute")
	switch s.sType {
	case STATEMENT_INSERT:
		return s.executeInsert(t)
	case STATEMENT_SELECT:
		return s.executeSelect(t)
	}
	return EXECUTE_SUCCESS
}

func (s *statement) executeInsert(table *table) statusCode {
	log("execute inser")
	if table.numOfRows >= s.rowToInsert.tableMaxRows {
		return EXECUTE_TABLE_FULL
	}
	s.rowToInsert.serialize(table)
	table.numOfRows++

	return EXECUTE_SUCCESS
}

func (s *statement) executeSelect(table *table) statusCode {
	var _row *row
	for i := uint32(0); i < table.numOfRows; i++ {
		//deserializer row
		_row = table.deserializeRow(i)
		//print row
		_row.print()
	}
	return EXECUTE_SUCCESS
}

func doMetaCommand(input string, t *table) statusCode {
	if strings.Compare(input, ".exit") == 0 {
		t.closeDB()
		fmt.Println("Bye")
		os.Exit(0)
	}

	return META_COMMAND_UNRECOGNIZED
}

func main() {
	fmt.Println("Hello World!!!")

	if len(os.Args) < 2 {
		fmt.Println("Invalid args")
		os.Exit(1)
	}

	filename := os.Args[1]

	table := openDB(filename)
	for {
		//print prompt
		printPrompt()
		//read line
		input := readLine()

		if strings.Compare(input[:1], ".") == 0 {
			switch doMetaCommand(input, table) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED:
				fmt.Println("Unrecognized command : ", input)
				continue
			}
		}

		_statement := NewStatement()

		switch _statement.prepareStatement(input) {
		case PREPARE_SUCCESS:
			//do nothing
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Println("Un recognized command")
			continue
		}

		switch _statement.execute(table) {
		case EXECUTE_SUCCESS:
			fmt.Println("Executed")
		case EXECUTE_TABLE_FULL:
			fmt.Println("Error: Table is full")
		}
		fmt.Println("Executed")

	}
}

func printPrompt() {
	fmt.Print("db> ")
}

//read input from user
func readLine() string {
	reader := bufio.NewReader(os.Stdin)
	text, _ := reader.ReadString('\n')
	text = strings.Replace(text, "\n", "", -1)
	return text
}

func log(data ...interface{}) {
	//fmt.Printf("LOG: %v \n", data)
}
