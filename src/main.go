package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
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

type page struct {
	page []byte
}

func NewPage() *page {
	p := &page{
		page: make([]byte, PAGE_SIZE),
	}
	return p
}

type table struct {
	num_rows uint32
	pages    []*page
}

func NewTable() *table {
	t := &table{
		num_rows: 0,
		pages:    make([]*page, TABLE_MAX_PAGE),
	}

	for i := 0; i < TABLE_MAX_PAGE; i++ {
		t.pages[i] = NewPage()
	}

	return t
}

func (t *table) rowSlot(rowNum uint32, r *row) (*page, uint32) {
	pageNum := rowNum / r.rowsPerPage
	log("pageNum", pageNum)
	page := t.pages[pageNum]
	if page == nil {
		//Allocate memory only when we try to access page
		t.pages[pageNum] = NewPage()
		page = t.pages[pageNum]
	}
	rowOffset := rowNum % r.rowsPerPage
	byteOffset := rowOffset * r.size
	log("byteOffset", byteOffset)
	return page, byteOffset
}

func (t *table) deserializeRow(rowNum uint32) *row {
	row := NewRow()
	page, offset := t.rowSlot(rowNum, row)
	offset = offset + row.idOffset
	row.id = binary.LittleEndian.Uint32(page.page[offset : offset+row.idSize])
	offset = offset + row.usernameOffset
	row.username = page.page[offset : offset+row.usernameSize]
	offset = offset + row.emailOffset
	row.email = page.page[offset : offset+row.emailSize]

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
	p, offset := t.rowSlot(t.num_rows, r)
	offset = offset + r.idOffset
	binary.LittleEndian.PutUint32(p.page[offset:offset+r.idSize], r.id)
	offset = offset + r.usernameOffset
	copy(p.page[offset:offset+r.usernameSize], r.username)
	offset = offset + r.emailOffset
	copy(p.page[offset:offset+r.emailSize], r.email)
}

func (r *row) print() {
	fmt.Println(r.id, string(r.username), string(r.email))
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
	if table.num_rows >= s.rowToInsert.tableMaxRows {
		return EXECUTE_TABLE_FULL
	}
	s.rowToInsert.serialize(table)
	table.num_rows++

	return EXECUTE_SUCCESS
}

func (s *statement) executeSelect(table *table) statusCode {
	var _row *row
	for i := uint32(0); i < table.num_rows; i++ {
		//deserializer row
		_row = table.deserializeRow(i)
		//print row
		_row.print()
	}
	return EXECUTE_SUCCESS
}

func doMetaCommand(input string) statusCode {
	if strings.Compare(input, ".exit") == 0 {
		fmt.Println("Bye")
		os.Exit(0)
	}

	return META_COMMAND_UNRECOGNIZED
}

func main() {
	fmt.Println("Hello World!!!")
	table := NewTable()
	for {
		//print prompt
		printPrompt()
		//read line
		input := readLine()

		if strings.Compare(input[:1], ".") == 0 {
			switch doMetaCommand(input) {
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
