ql driver and dialect for github.com/go-xorm/xorm
========

** experiment support **

STATUS: build pass but some tests failed.

Currently, we can support ql for some operations.

The below operation cannot be supported because of ql's limitation:

* Non-interger Primary Key & Composite Primary Key
* Column's name and Tables' name could not be ql's keywords, you can visit [github.com/cznic/ql](http://github.com/cznic/ql) to check out it.

# How to use

Just like other supports of xorm, but you should import the three packages:

Since github.com/cznic/ql# has not been resolved, we just use github.com/lunny/ql instead.

```Go
import (
	_ "github.com/lunny/ql/driver"
	_ "github.com/go-xorm/ql"
	"github.com/go-xorm/xorm"
)

// for open a file
xorm.NewEngine("ql", "./ql.db")

// for open a memory file
xorm.NewEngine("ql-mem", "./ql.db")
```