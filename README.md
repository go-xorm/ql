ql driver and dialect for github.com/go-xorm/xorm
========

STATUS: build pass but some tests failed.

Currently, we can support ql for some operations.

The below operation cannot be supported because of ql's limitation:

* Composite Primary Key
* Non interger Primary Key
* Copmosite index and unique index
* Column's name and Tables' name could not be ql's keywords, you can visit [github.com/cznic/ql](http://github.com/cznic/ql) to check out it.

＃ How to use

Just like other supports of xorm, but you should import the three packages:

```Go
import (
	_ "github.com/cznic/ql/driver"
	_ "github.com/go-xorm/ql"
	"github.com/go-xorm/xorm"
)

// for open a file
xorm.NewEngine("ql", "./ql.db")

// for open a memory file
xorm.NewEngine("ql-mem", "./ql.db")
```