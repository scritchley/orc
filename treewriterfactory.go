package orc

import (
	"fmt"

	"code.simon-critchley.co.uk/orc/proto"
)

func createTreeWriter(codec CompressionCodec, schema *TypeDescription, m streamWriterMap, encodings encodingMap,
	statistics, indexStatistics statisticsMap) (TreeWriter, error) {

	id := schema.getID()
	var treeWriter TreeWriter
	var err error
	category := schema.getCategory()
	stats := statistics.add(id, category)
	indexStats := indexStatistics.add(id, category)
	switch category {
	case CategoryFloat:
		treeWriter, err = NewFloatTreeWriter(
			m.create(codec, streamName{id, proto.Stream_PRESENT}),
			m.create(codec, streamName{id, proto.Stream_DATA}),
			stats,
			indexStats,
			4,
		)
		if err != nil {
			return nil, err
		}
	case CategoryDouble:
		treeWriter, err = NewFloatTreeWriter(
			m.create(codec, streamName{id, proto.Stream_PRESENT}),
			m.create(codec, streamName{id, proto.Stream_DATA}),
			stats,
			indexStats,
			8,
		)
		if err != nil {
			return nil, err
		}
	case CategoryBoolean:
		treeWriter, err = NewBooleanTreeWriter(
			m.create(codec, streamName{id, proto.Stream_PRESENT}),
			m.create(codec, streamName{id, proto.Stream_DATA}),
			stats,
			indexStats,
		)
		if err != nil {
			return nil, err
		}
	case CategoryStruct:
		// Create a TreeWriter for each child of the struct column.
		var children []TreeWriter
		for _, child := range schema.children {
			childWriter, err := createTreeWriter(codec, child, m, encodings, statistics, indexStatistics)
			if err != nil {
				return nil, err
			}
			children = append(children, childWriter)
		}
		treeWriter, err = NewStructTreeWriter(
			m.create(codec, streamName{id, proto.Stream_PRESENT}),
			children,
			stats,
			indexStats,
		)
		if err != nil {
			return nil, err
		}
	case CategoryShort, CategoryInt, CategoryLong:
		treeWriter, err = NewIntegerTreeWriter(
			m.create(codec, streamName{id, proto.Stream_PRESENT}),
			m.create(codec, streamName{id, proto.Stream_DATA}),
			stats,
			indexStats,
		)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported type: %s", category)
	}
	// Add the writers encoding to the encoding map.
	encodings.add(id, treeWriter.ColumnEncoding())
	// Return the TreeWriter
	return treeWriter, nil
}
