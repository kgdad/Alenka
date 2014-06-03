#include "jdbc.h"
#include "cm.h"

#ifdef _WIN64
#define atoll(S) _atoi64(S)
#include <windows.h>
#else
#include <unistd.h>
#endif

JDBC::JDBC(CudaSet* cs) {
	cudaSet = cs;
}
;

int JDBC::getRecordCount() {
	return cudaSet->mRecCount;
}
;

int JDBC::getColumnCount() {
	return cudaSet->mColumnCount;
}
;

void JDBC::getColumnNames(char** colNames) {
	for (size_t i = 0; i < cudaSet->columnNames.size(); i++) {
		//colNames[i] = new char[cudaSet->columnNames[i].size() +1];
		strcpy(colNames[i], cudaSet->columnNames[i].c_str());
		cout << colNames[i] << endl;
	}
}
;

void JDBC::getColumnName(int colNum, char* colName) {
	if (colNum < 0 || colNum >= cudaSet->mColumnCount) {
		colName = NULL;
	} else {
		strcpy(colName, cudaSet->columnNames[colNum].c_str());
	}
}
;

JDBC::alenka_types JDBC::getColumnTypes(int colNum) {
	if(cudaSet->type[cudaSet->columnNames[colNum]] == 0) {
		return alenka_integer;
	} else if(cudaSet->type[cudaSet->columnNames[colNum]] == 1){
		return alenka_float;
	} else {
		return alenka_char;
	}
}
;

size_t JDBC::getColumnSize(int colNum) {
	return cudaSet->char_size[cudaSet->columnNames[colNum]];
}
;

void JDBC::retrieveChar(int rowNum, int colNum, char* data) {
	if(rowNum < 0 || rowNum > cudaSet->mRecCount) {
		cout << "Invalid Row Number" << endl;
		data = NULL;
		return;
	}

	if(colNum < 0 || colNum > cudaSet->columnNames.size()) {
		cout << "Invalid Column Number" << endl;
		data = NULL;
		return;
	}

	strncpy(data, cudaSet->h_columns_char[cudaSet->columnNames[colNum]] + (rowNum*cudaSet->char_size[cudaSet->columnNames[colNum]]), cudaSet->char_size[cudaSet->columnNames[colNum]]);
}

long long int JDBC::retrieveInt(int rowNum, int colNum) {
	if(rowNum < 0 || rowNum > cudaSet->mRecCount) {
		cout << "Invalid Row Number" << endl;
		return 0;
	}

	if(colNum < 0 || colNum > cudaSet->columnNames.size()) {
		cout << "Invalid Column Number" << endl;
		return 0;
	}

	return (cudaSet->h_columns_int[cudaSet->columnNames[colNum]])[rowNum];
}

double JDBC::retrieveFloat(int rowNum, int colNum) {
	if(rowNum < 0 || rowNum > cudaSet->mRecCount) {
		cout << "Invalid Row Number" << endl;
		return 0;
	}

	if(colNum < 0 || colNum > cudaSet->columnNames.size()) {
		cout << "Invalid Column Number" << endl;
		return 0;
	}
	return (cudaSet->h_columns_float[cudaSet->columnNames[colNum]])[rowNum];
}

void JDBC::retrieveRow(int rowNum) {
	//for now success is printing out the column names and the next row of data
	cout << "Retrieving the row from the cudaset" << endl;
	for (unsigned int i = 0; i < cudaSet->columnNames.size(); i++) {
		if (cudaSet->type[cudaSet->columnNames[i]] == 0)
			cout << cudaSet->h_columns_int[cudaSet->columnNames[i]][rowNum];
		else if (cudaSet->type[cudaSet->columnNames[i]] == 1)
			cout << cudaSet->h_columns_float[cudaSet->columnNames[i]][rowNum];
		else
			cout
					<< cudaSet->h_columns_char[cudaSet->columnNames[i]]
							+ (i * cudaSet->char_size[cudaSet->columnNames[i]]), cudaSet->char_size[cudaSet->columnNames[i]];

		cout << "  |  ";
	}

	cout << endl;
}
;
