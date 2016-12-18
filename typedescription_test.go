package orc

import (
	"testing"
)

func TestTypeDescriptionParse(t *testing.T) {

	description := NewStringPosition("struct<f1:int,f2:string,f3:decimal(38,10)>")

	td, err := description.parseType()
	if err != nil {
		t.Fatal(err)
	}

	expected := "struct<f1:int,f2:string,f3:decimal(38,10)>"
	if td.String() != expected {
		t.Errorf("Test failed, expected %s got %s", expected, td.String())
	}

	expectedJSON := `{"category": "struct", "id": 0, "max": 3, "fields": {"f1": {"category": "int", "id": 1, "max": 1},"f2": {"category": "string", "id": 2, "max": 2},"f3": {"category": "decimal", "id": 3, "max": 3, "precision": 38, "scale": 10}}}`
	if td.ToJSON() != expectedJSON {
		t.Errorf("Test failed, expected %s got %s", expected, td.ToJSON())
	}

	description = NewStringPosition(`struct<f1:int,f2:string,f3:decimal(38,10),f4:array<struct<f5:int,f6:string>>>`)

	td, err = description.parseType()
	if err != nil {
		t.Fatal(err)
	}

	expected = "struct<f1:int,f2:string,f3:decimal(38,10),f4:array<struct<f5:int,f6:string>>>"
	if td.String() != expected {
		t.Errorf("Test failed, expected %s got %s", expected, td.String())
	}

	expectedJSON = `{"category": "struct", "id": 0, "max": 7, "fields": {"f1": {"category": "int", "id": 1, "max": 1},"f2": {"category": "string", "id": 2, "max": 2},"f3": {"category": "decimal", "id": 3, "max": 3, "precision": 38, "scale": 10},"f4": {"category": "array", "id": 4, "max": 7, "children": [{"category": "struct", "id": 5, "max": 7, "fields": {"f5": {"category": "int", "id": 6, "max": 6},"f6": {"category": "string", "id": 7, "max": 7}}}]}}}`
	if td.ToJSON() != expectedJSON {
		t.Errorf("Test failed, expected %s got %s", expected, td.ToJSON())
	}

	description = NewStringPosition(`STRUCT<
		f1: MAP<STRING,STRING>
	>`)

	td, err = description.parseType()
	if err != nil {
		t.Fatal(err)
	}

	expected = `struct<f1:map<string,string>>`
	if td.String() != expected {
		t.Errorf("Test failed, expected %s got %s", expected, td.String())
	}

	expectedJSON = `{"category": "struct", "id": 0, "max": 3, "fields": {"f1": {"category": "map", "id": 1, "max": 3, "children": [{"category": "string", "id": 2, "max": 2},{"category": "string", "id": 3, "max": 3}]}}}`
	if td.ToJSON() != expectedJSON {
		t.Errorf("Test failed, expected %s got %s", expected, td.ToJSON())
	}

}

func TestTypeDescriptionPrint(t *testing.T) {

	td, err := NewTypeDescription(
		SetCategory(CategoryStruct),
		AddField(
			"f1",
			SetCategory(CategoryInt),
		),
		AddField(
			"f2",
			SetCategory(CategoryString),
		),
		AddField(
			"f3",
			SetCategory(CategoryDecimal),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	expected := "struct<f1:int,f2:string,f3:decimal(38,10)>"
	if td.String() != expected {
		t.Errorf("Test failed, expected %s got %s", expected, td.String())
	}

	expected = `{"category": "struct", "id": 0, "max": 3, "fields": {"f1": {"category": "int", "id": 1, "max": 1},"f2": {"category": "string", "id": 2, "max": 2},"f3": {"category": "decimal", "id": 3, "max": 3, "precision": 38, "scale": 10}}}`
	if td.ToJSON() != expected {
		t.Errorf("Test failed, expected %s got %s", expected, td.ToJSON())
	}

	td2, err := NewTypeDescription(
		SetCategory(CategoryStruct),
		AddField(
			"f1",
			SetCategory(CategoryUnion),
			AddUnionChild(
				SetCategory(CategoryByte),
			),
			AddUnionChild(
				SetCategory(CategoryDecimal),
			),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	expected = "struct<f1:uniontype<tinyint,decimal(38,10)>>"
	if td2.String() != expected {
		t.Errorf("Test failed, expected %s got %s", expected, td2.String())
	}

	expected = `{"category": "struct", "id": 0, "max": 3, "fields": {"f1": {"category": "uniontype", "id": 1, "max": 3, "children": [{"category": "tinyint", "id": 2, "max": 2},{"category": "decimal", "id": 3, "max": 3, "precision": 38, "scale": 10}]}}}`
	if td2.ToJSON() != expected {
		t.Errorf("Test failed, expected %s got %s", expected, td2.ToJSON())
	}

}
