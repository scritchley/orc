package orc

import (
	"fmt"
)

func createTreeWriter(codec CompressionCodec, schema *TypeDescription, writers writerMap) (TreeWriter, error) {

	id := schema.getID()
	var treeWriter TreeWriter
	var err error
	category := schema.getCategory()
	switch category {
	case CategoryFloat:
		treeWriter, err = NewFloatTreeWriter(category, codec, 4)
		if err != nil {
			return nil, err
		}
	case CategoryDouble:
		treeWriter, err = NewFloatTreeWriter(category, codec, 8)
		if err != nil {
			return nil, err
		}
	case CategoryBoolean:
		treeWriter, err = NewBooleanTreeWriter(category, codec)
		if err != nil {
			return nil, err
		}
	case CategoryStruct:
		// Create a TreeWriter for each child of the struct column.
		var children []TreeWriter
		for _, child := range schema.children {
			childWriter, err := createTreeWriter(codec, child, writers)
			if err != nil {
				return nil, err
			}
			children = append(children, childWriter)
		}
		treeWriter, err = NewStructTreeWriter(category, codec, children)
		if err != nil {
			return nil, err
		}
	case CategoryShort, CategoryInt, CategoryLong:
		treeWriter, err = NewIntegerTreeWriter(category, codec)
		if err != nil {
			return nil, err
		}
	case CategoryVarchar, CategoryString:
		treeWriter, err = NewStringTreeWriter(category, codec)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported type: %s", category)
	}
	writers.add(id, treeWriter)
	// Return the TreeWriter
	return treeWriter, nil
}
