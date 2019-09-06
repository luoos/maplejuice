package result

import (
	"fmt"
)

func PrintResult(result MachineResult) {
	m_name := result.Name
	for _, f := range result.Files {
		f_name := f.Name
		for _, l := range f.Lines {
			fmt.Println(m_name + ":" + f_name + ":" + l[0] + ":" + l[1])
		}
	}
}
