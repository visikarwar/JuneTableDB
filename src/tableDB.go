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

var ROW *row = NewRow()

type cursor struct {
	table      *table
	rowNum     uint32
	endOfTable bool
}

func NewCursor() *cursor {
	c := &cursor{}
	return c
}

func (c *cursor) cursorValue() (*page, uint32) {
	pageNum := c.rowNum / ROW.rowsPerPage
	page := c.table.pager.getPage(pageNum)

	rowOffset := c.rowNum % ROW.rowsPerPage
	byteOffset := rowOffset * ROW.size
	return page, byteOffset

}

func (c *cursor) advance() {
	c.rowNum++
	if c.rowNum >= c.table.numOfRows {
		c.endOfTable = true
	}
}

type pager struct {
	fd         *os.File
	fileLength uint32
	pages      []*page
}

func NewPager(filename string) *pager {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, syscall.S_IWUSR|syscall.S_IRUSR)

	if err != nil {
		log(1, "Unable to open file")
		os.Exit(1)
	}

	fs, err := file.Stat()
	if err != nil {
		log(1, "Unable to fetch stat")
	}

	p := &pager{}
	p.fileLength = uint32(fs.Size())
	p.fd = file
	p.pages = make([]*page, TABLE_MAX_PAGE)

	return p
}

func (p *pager) getPage(pageNum uint32) *page {
	if pageNum > TABLE_MAX_PAGE {
		log(1, "Tried to fetch page number out of bound", TABLE_MAX_PAGE)
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
			log(2, "seeked to offset", o)
			_, e := p.fd.Read(page.bytes)
			if e != nil {
				log(2, "Error while reading bytes from file")
			}
			p.pages[pageNum] = page
		}
	}

	return p.pages[pageNum]
}

func (p *pager) flush(pageNum uint32, size uint32) {
	if p.pages[pageNum] == nil {
		log(1, "Tried to flush null")
		os.Exit(1)
	}

	_, err := p.fd.Seek(int64(pageNum*PAGE_SIZE), io.SeekStart)
	if err != nil {
		log(1, "Error seeking file")
		os.Exit(1)
	}

	_, err = p.fd.Write(p.pages[pageNum].bytes[:size])
	if err != nil {
		log(1, "Error writing")
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
	t.numOfRows = t.pager.fileLength / ROW.size
	return t
}

func (t *table) start() *cursor {
	c := NewCursor()
	c.table = t
	c.rowNum = 0
	c.endOfTable = (t.numOfRows == 0)
	return c
}

func (t *table) end() *cursor {
	c := NewCursor()
	c.table = t
	c.rowNum = t.numOfRows
	c.endOfTable = true
	return c
}

func (t *table) closeDB() {

	numOfFullPages := t.numOfRows / ROW.rowsPerPage

	for i := 0; i < int(numOfFullPages); i++ {
		if t.pager.pages[i] == nil {
			continue
		}
		t.pager.flush(uint32(i), PAGE_SIZE)
		t.pager.pages[i] = nil
	}

	//there may be a partial page to write to the end of the file
	//this should not be needed after we switch to B-Tree
	additionalRows := t.numOfRows % ROW.rowsPerPage
	if additionalRows > 0 {
		pageNum := numOfFullPages
		if t.pager.pages[pageNum] != nil {
			t.pager.flush(pageNum, additionalRows*ROW.size)
			t.pager.pages[pageNum] = nil
		}
	}

	err := t.pager.fd.Close()
	if err != nil {
		log(1, "Error while closing fd")
		os.Exit(1)
	}
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

func (r *row) serialize(c *cursor) {
	p, offset := c.cursorValue()

	offset = offset + r.idOffset
	binary.LittleEndian.PutUint32(p.bytes[offset:offset+r.idSize], r.id)
	offset = offset + r.usernameOffset
	copy(p.bytes[offset:offset+r.usernameSize], r.username)
	offset = offset + r.emailOffset
	copy(p.bytes[offset:offset+r.emailSize], r.email)
}

func (r *row) deserialize(c *cursor) {
	page, offset := c.cursorValue()
	offset = offset + r.idOffset
	r.id = binary.LittleEndian.Uint32(page.bytes[offset : offset+r.idSize])
	offset = offset + r.usernameOffset
	r.username = page.bytes[offset : offset+r.usernameSize]
	offset = offset + r.emailOffset
	r.email = page.bytes[offset : offset+r.emailSize]
}

func (r *row) print() {
	log(0, r.id, string(r.username), string(r.email))
}

/*func (r *row) toString() string {
	var buffer bytes.Buffer
	buffer.WriteString(strconv.Itoa(int(r.id)))
	buffer.WriteString(string(r.username))
	buffer.WriteString(string(r.email))
	return buffer.String()
}*/

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
			return PREPARE_SYNTAX_ERROR
		}
		s.rowToInsert.id = uint32(id)
		copy(s.rowToInsert.username, inputarray[2])
		copy(s.rowToInsert.email, inputarray[3])
		s.rowToInsert.print()
		return PREPARE_SUCCESS
	}

	if strings.Compare(input, "select") == 0 {
		s.sType = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}

	return PREPARE_UNRECOGNIZED_STATEMENT
}

func (s *statement) execute(t *table) statusCode {
	switch s.sType {
	case STATEMENT_INSERT:
		return s.executeInsert(t)
	case STATEMENT_SELECT:
		return s.executeSelect(t)
	}
	return EXECUTE_SUCCESS
}

func (s *statement) executeInsert(table *table) statusCode {
	if table.numOfRows >= s.rowToInsert.tableMaxRows {
		return EXECUTE_TABLE_FULL
	}
	c := table.end()
	s.rowToInsert.serialize(c)
	table.numOfRows++

	return EXECUTE_SUCCESS
}

func (s *statement) executeSelect(table *table) statusCode {
	c := table.start()

	for !c.endOfTable {
		r := NewRow()
		r.deserialize(c)
		r.print()
		c.advance()
	}

	return EXECUTE_SUCCESS
}

func doMetaCommand(input string, t *table) statusCode {
	if strings.Compare(input, ".exit") == 0 {
		t.closeDB()
		log(0, "Bye")
		os.Exit(0)
	}

	return META_COMMAND_UNRECOGNIZED
}

func main() {
	log(0, "June Table DB!!!")

	if len(os.Args) < 2 {
		log(1, "Invalid args")
		os.Exit(1)
	}

	filename := os.Args[1]
	processDB(filename)

}

func processDB(filename string) {
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
				log(1, "Unrecognized command : ", input)
				continue
			}
		}

		_statement := NewStatement()

		switch _statement.prepareStatement(input) {
		case PREPARE_SUCCESS:
			//do nothing
		case PREPARE_UNRECOGNIZED_STATEMENT:
			log(1, "Unrecognized command")
			continue
		}

		switch _statement.execute(table) {
		case EXECUTE_SUCCESS:
			log(0, "Executed")
		case EXECUTE_TABLE_FULL:
			log(1, "Error: Table is full")
		}
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

func log(level int, data ...interface{}) {
	if level == 0 {
		fmt.Printf(" %v \n", data)
	} else if level == 1 {
		fmt.Printf("Error LOG : %v \n", data)
	} else if level == 2 {
		fmt.Printf("Info LOG : %v \n", data)
	}
}
