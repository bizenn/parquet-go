package Layout

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/xitongsys/parquet-go/Common"
	"github.com/xitongsys/parquet-go/Compress"
	"github.com/xitongsys/parquet-go/ParquetEncoding"
	"github.com/xitongsys/parquet-go/ParquetType"
	"github.com/xitongsys/parquet-go/parquet"
	"reflect"
)

//Page is used to store the page data
type Page struct {
	//Header of a page
	Header *parquet.PageHeader
	//Table to store values
	DataTable *Common.Table
	//Compressed data of the page, which is written in parquet file
	RawData []byte
	//Compress type: gzip/snappy/none
	CompressType parquet.CompressionCodec
	//Parquet type of the values in the page
	DataType parquet.Type
	//Maximum of the values
	MaxVal interface{}
	//Minimum of the values
	MinVal interface{}
}

//Create a new page
func NewPage() *Page {
	page := new(Page)
	page.DataTable = nil
	page.Header = parquet.NewPageHeader()
	return page
}

//Create a new dict page
func NewDictPage() *Page {
	page := NewPage()
	page.Header.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
	return page
}

//Create a new data page
func NewDataPage() *Page {
	page := NewPage()
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	return page
}

//Convert a table to data pages
func TableToDataPages(table *Common.Table, pageSize int32, compressType parquet.CompressionCodec) ([]*Page, int64) {
	var totSize int64 = 0
	totalLn := len(table.Values)
	res := make([]*Page, 0)
	i := 0
	dataType := table.Type

	for i < totalLn {
		j := i + 1
		var size int32 = 0
		var numValues int32 = 0

		var maxVal interface{} = table.Values[i]
		var minVal interface{} = table.Values[i]

		for j < totalLn && size < pageSize {
			if table.DefinitionLevels[j] == table.MaxDefinitionLevel {
				numValues++
				size += int32(Common.SizeOf(reflect.ValueOf(table.Values[j])))
				maxVal = Common.Max(maxVal, table.Values[j])
				minVal = Common.Min(minVal, table.Values[j])
			}
			j++
		}

		page := NewDataPage()
		page.Header.DataPageHeader.NumValues = numValues
		page.Header.Type = parquet.PageType_DATA_PAGE

		page.DataTable = new(Common.Table)
		page.DataTable.RepetitionType = table.RepetitionType
		page.DataTable.Path = table.Path
		page.DataTable.MaxDefinitionLevel = table.MaxDefinitionLevel
		page.DataTable.MaxRepetitionLevel = table.MaxRepetitionLevel
		page.DataTable.Values = table.Values[i:j]
		page.DataTable.DefinitionLevels = table.DefinitionLevels[i:j]
		page.DataTable.RepetitionLevels = table.RepetitionLevels[i:j]
		page.MaxVal = maxVal
		page.MinVal = minVal
		page.DataType = dataType
		page.CompressType = compressType

		page.DataPageCompress(compressType)

		totSize += int64(len(page.RawData))
		res = append(res, page)
		i = j
	}

	return res, totSize
}

//Compress the data page to parquet file
func (page *Page) DataPageCompress(compressType parquet.CompressionCodec) []byte {
	ln := len(page.DataTable.DefinitionLevels)

	//values////////////////////////////////////////////
	valuesBuf := make([]interface{}, 0)
	for i := 0; i < ln; i++ {
		if page.DataTable.DefinitionLevels[i] == page.DataTable.MaxDefinitionLevel {
			valuesBuf = append(valuesBuf, page.DataTable.Values[i])
		}
	}
	valuesRawBuf := ParquetEncoding.WritePlain(valuesBuf)

	/*
		////////test DeltaINT64///////////////////
		if page.DataType == parquet.Type_INT64 {
			//valuesRawBuf = WriteDeltaINT64(valuesBuf)
			//log.Println(valuesRawBuf)
		}
		if page.DataType == parquet.Type_INT32 {
			//valuesRawBuf = WriteDeltaINT32(valuesBuf)
			//log.Println("++++++", valuesRawBuf)
		}
		if page.DataType == parquet.Type_BYTE_ARRAY {
			valuesRawBuf = WriteDeltaByteArray(valuesBuf)
		}
		////////////////////////////////////////////
	*/
	//definitionLevel//////////////////////////////////
	var definitionLevelBuf []byte
	if page.DataTable.MaxDefinitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = ParquetType.INT64(page.DataTable.DefinitionLevels[i])
		}
		definitionLevelBuf = ParquetEncoding.WriteRLEBitPackedHybrid(numInterfaces, int32(Common.BitNum(uint64(page.DataTable.MaxDefinitionLevel))))
	}

	//repetitionLevel/////////////////////////////////
	var repetitionLevelBuf []byte
	if page.DataTable.MaxRepetitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = ParquetType.INT64(page.DataTable.RepetitionLevels[i])
		}
		repetitionLevelBuf = ParquetEncoding.WriteRLEBitPackedHybrid(numInterfaces, int32(Common.BitNum(uint64(page.DataTable.MaxRepetitionLevel))))
	}

	//dataBuf = repetitionBuf + definitionBuf + valuesRawBuf
	var dataBuf []byte
	dataBuf = append(dataBuf, repetitionLevelBuf...)
	dataBuf = append(dataBuf, definitionLevelBuf...)
	dataBuf = append(dataBuf, valuesRawBuf...)

	var dataEncodeBuf []byte
	if compressType == parquet.CompressionCodec_GZIP {
		dataEncodeBuf = Compress.CompressGzip(dataBuf)
	} else if compressType == parquet.CompressionCodec_SNAPPY {
		dataEncodeBuf = Compress.CompressSnappy(dataBuf)
	} else {
		dataEncodeBuf = dataBuf
	}

	//pageHeader/////////////////////////////////////
	page.Header = parquet.NewPageHeader()
	page.Header.Type = parquet.PageType_DATA_PAGE
	page.Header.CompressedPageSize = int32(len(dataEncodeBuf))
	page.Header.UncompressedPageSize = int32(len(dataBuf))
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	page.Header.DataPageHeader.NumValues = int32(len(page.DataTable.Values))
	page.Header.DataPageHeader.DefinitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.RepetitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.Encoding = parquet.Encoding_PLAIN

	/*
		/////////test DeltaINT64////////////////
		if page.DataType == parquet.Type_INT64 {
			//page.Header.DataPageHeader.Encoding = parquet.Encoding_DELTA_BINARY_PACKED
		}
		if page.DataType == parquet.Type_INT32 {
			page.Header.DataPageHeader.Encoding = parquet.Encoding_DELTA_BINARY_PACKED
		}
		if page.DataType == parquet.Type_BYTE_ARRAY {
			page.Header.DataPageHeader.Encoding = parquet.Encoding_DELTA_BYTE_ARRAY
		}
		//////////////////////////////////////
	*/
	page.Header.DataPageHeader.Statistics = parquet.NewStatistics()
	if page.MaxVal != nil {
		tmpBuf := ParquetEncoding.WritePlain([]interface{}{page.MaxVal})
		name := reflect.TypeOf(page.MaxVal).Name()
		if name == "UTF8" || name == "DECIMAL" {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeader.Statistics.Max = tmpBuf
	}
	if page.MinVal != nil {
		tmpBuf := ParquetEncoding.WritePlain([]interface{}{page.MinVal})
		name := reflect.TypeOf(page.MinVal).Name()
		if name == "UTF8" || name == "DECIMAL" {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeader.Statistics.Min = tmpBuf
	}

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(page.Header)

	var res []byte
	res = append(res, pageHeaderBuf...)
	res = append(res, dataEncodeBuf...)
	page.RawData = res

	return res
}

//Compress data page v2 to parquet file
func (page *Page) DataPageV2Compress(compressType parquet.CompressionCodec) []byte {
	ln := len(page.DataTable.DefinitionLevels)

	//values////////////////////////////////////////////
	valuesBuf := make([]interface{}, 0)
	for i := 0; i < ln; i++ {
		if page.DataTable.DefinitionLevels[i] == page.DataTable.MaxDefinitionLevel {
			valuesBuf = append(valuesBuf, page.DataTable.Values[i])
		}
	}
	valuesRawBuf := ParquetEncoding.WritePlain(valuesBuf)

	//definitionLevel//////////////////////////////////
	var definitionLevelBuf []byte
	if page.DataTable.MaxDefinitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = ParquetType.INT64(page.DataTable.DefinitionLevels[i])
		}
		definitionLevelBuf = ParquetEncoding.WriteRLE(numInterfaces, int32(Common.BitNum(uint64(page.DataTable.MaxDefinitionLevel))))
	}

	//repetitionLevel/////////////////////////////////
	r0Num := int32(0)
	var repetitionLevelBuf []byte
	if page.DataTable.MaxRepetitionLevel > 0 {
		numInterfaces := make([]interface{}, ln)
		for i := 0; i < ln; i++ {
			numInterfaces[i] = ParquetType.INT64(page.DataTable.RepetitionLevels[i])
			if page.DataTable.RepetitionLevels[i] == 0 {
				r0Num++
			}
		}
		repetitionLevelBuf = ParquetEncoding.WriteRLE(numInterfaces, int32(Common.BitNum(uint64(page.DataTable.MaxRepetitionLevel))))
	}

	var dataEncodeBuf []byte
	if compressType == parquet.CompressionCodec_GZIP {
		dataEncodeBuf = Compress.CompressGzip(valuesRawBuf)
	} else if compressType == parquet.CompressionCodec_SNAPPY {
		dataEncodeBuf = Compress.CompressSnappy(valuesRawBuf)
	} else {
		dataEncodeBuf = valuesRawBuf
	}

	//pageHeader/////////////////////////////////////
	page.Header = parquet.NewPageHeader()
	page.Header.Type = parquet.PageType_DATA_PAGE_V2
	page.Header.CompressedPageSize = int32(len(dataEncodeBuf) + len(definitionLevelBuf) + len(repetitionLevelBuf))
	page.Header.UncompressedPageSize = int32(len(valuesRawBuf) + len(definitionLevelBuf) + len(repetitionLevelBuf))
	page.Header.DataPageHeaderV2 = parquet.NewDataPageHeaderV2()
	page.Header.DataPageHeaderV2.NumValues = int32(len(page.DataTable.Values))
	page.Header.DataPageHeaderV2.NumNulls = page.Header.DataPageHeaderV2.NumValues - int32(len(valuesBuf))
	page.Header.DataPageHeaderV2.NumRows = r0Num
	page.Header.DataPageHeaderV2.Encoding = parquet.Encoding_PLAIN
	page.Header.DataPageHeaderV2.DefinitionLevelsByteLength = int32(len(definitionLevelBuf))
	page.Header.DataPageHeaderV2.RepetitionLevelsByteLength = int32(len(repetitionLevelBuf))
	page.Header.DataPageHeaderV2.IsCompressed = true

	page.Header.DataPageHeaderV2.Statistics = parquet.NewStatistics()
	if page.MaxVal != nil {
		tmpBuf := ParquetEncoding.WritePlain([]interface{}{page.MaxVal})
		name := reflect.TypeOf(page.MaxVal).Name()
		if name == "UTF8" || name == "DECIMAL" {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeaderV2.Statistics.Max = tmpBuf
	}
	if page.MinVal != nil {
		tmpBuf := ParquetEncoding.WritePlain([]interface{}{page.MinVal})
		name := reflect.TypeOf(page.MinVal).Name()
		if name == "UTF8" || name == "DECIMAL" {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeaderV2.Statistics.Min = tmpBuf
	}

	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
	pageHeaderBuf, _ := ts.Write(page.Header)

	var res []byte
	res = append(res, pageHeaderBuf...)
	res = append(res, repetitionLevelBuf...)
	res = append(res, definitionLevelBuf...)
	res = append(res, dataEncodeBuf...)
	page.RawData = res

	return res
}

//Convert table to dict data page. ToDo
func TableToDictDataPages(table *Common.Table, pageSize int32, compressType parquet.CompressionCodec) ([]*Page, int64) {
	return []*Page{}, 0
}

//Compress dict page. ToDo
func (page *Page) DictPageCompress(compressType parquet.CompressionCodec) []byte {
	return []byte{}
}
