/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#define EPSILON    (1.0E-8)

#include <queue>
#include <string>
#include <map>
#include <set>
#include <vector>
#include <stack>
#include <ctime>
#include <limits>
#include <fstream>

typedef long long int int_type;
typedef unsigned int int32_type;
typedef unsigned short int int16_type;
typedef char int8_type;

typedef double float_type;

using namespace std;
using namespace mgpu;

extern size_t int_size;
extern size_t float_size;
extern unsigned int hash_seed;
extern queue<string> op_type;
extern bool op_case;
extern queue<string> op_sort;
extern queue<string> op_presort;
extern queue<string> op_value;
extern queue<int_type> op_nums;
extern queue<float_type> op_nums_f;
extern queue<string> col_aliases;
extern size_t total_count, oldCount, total_max, totalRecs, alloced_sz;
extern unsigned int total_segments;
extern unsigned int process_count;
extern bool fact_file_loaded;
extern void* alloced_tmp;
extern unsigned int partition_count;
extern map<string,string> setMap; //map to keep track of column names and set names
extern std::clock_t tot;
extern std::clock_t tot_fil;
extern bool verbose;
extern bool save_dict;
extern bool interactive;
extern map<string, char*> buffers;
extern map<string, size_t> buffer_sizes;
extern queue<string> buffer_names;
extern size_t total_buffer_size;
extern unsigned long long int* raw_decomp;
extern unsigned int raw_decomp_length;
extern size_t alloced_sz;
extern void* alloced_tmp;


struct col_data {
	unsigned int col_type;
	unsigned int col_length;
};
extern map<string, map<string, col_data> > data_dict;

class CudaSet
{
public:
    map<string, char*> h_columns_char;

    map<string, char*> d_columns_char;		
	
    map<string, size_t> char_size;
    char prm_index; // A - all segments values match, N - none match, R - some may match

    // to do filters in-place (during joins, groupby etc ops) we need to save a state of several queues's and a few misc. variables:
    char* fil_s;
    char* fil_f;
    queue<string> fil_type,fil_value;
    queue<int_type> fil_nums;
    queue<float_type> fil_nums_f;

    //map<unsigned int, size_t> type_index;
    size_t mRecCount, maxRecs, hostRecCount, devRecCount, grp_count, segCount, prealloc_char_size, totalRecs;
    vector<string> columnNames;
	map<string,bool> compTypes; // pfor delta or not
    map<string, FILE*> filePointers;
    bool *grp;
    queue<string> columnGroups;
    bool not_compressed; // 1 = host recs are not compressed, 0 = compressed
    unsigned int mColumnCount;
    string name, load_file_name, separator, source_name;
    bool source, text_source, tmp_table, keep, filtered;
    queue<string> sorted_fields; //segment is sorted by fields
    queue<string> presorted_fields; //globally sorted by fields
    map<string, unsigned int> type; // 0 - integer, 1-float_type, 2-char
    map<string, bool> decimal; // column is decimal - affects only compression
    map<string, unsigned int> grp_type; // type of group : SUM, AVG, COUNT etc
    map<string, unsigned int> cols; // column positions in a file
	
	//alternative to Bloom filters. Keep track of non-empty segment join results ( not the actual results
	//but just boolean indicators.
	map<string, string> ref_sets; // referencing datasets
	map<string, string> ref_cols; // referencing dataset's column names
	map<string, map<unsigned int, set<unsigned int> > > ref_joins; // columns referencing dataset segments 
	map<string, set<unsigned int> > orig_segs;

    CudaSet(queue<string> &nameRef, queue<string> &typeRef, queue<int> &sizeRef, queue<int> &colsRef, size_t Recs, queue<string> &references, queue<string> &references_names);
    CudaSet(queue<string> &nameRef, queue<string> &typeRef, queue<int> &sizeRef, queue<int> &colsRef, size_t Recs, string file_name, unsigned int max);
    CudaSet(size_t RecordCount, unsigned int ColumnCount);
    CudaSet(CudaSet* a, CudaSet* b, queue<string> op_sel, queue<string> op_sel_as);    
    ~CudaSet();
    void allocColumnOnDevice(string colname, size_t RecordCount);
    void decompress_char_hash(string colname, unsigned int segment);
    void add_hashed_strings(string field, unsigned int segment);
    void resize(size_t addRecs);
    void resize_join(size_t addRecs);
    void reserve(size_t Recs);
    void deAllocColumnOnDevice(string colIndex);
    void allocOnDevice(size_t RecordCount);
    void deAllocOnDevice();
    void resizeDeviceColumn(size_t RecCount, string colIndex);
    void resizeDevice(size_t RecCount);
    bool onDevice(string colname);
    CudaSet* copyDeviceStruct();
    void readSegmentsFromFile(unsigned int segNum, string colname, size_t offset);
    void decompress_char(FILE* f, string colname, unsigned int segNum, size_t offset, char* mem);
    void CopyColumnToGpu(string colname,  unsigned int segment, size_t offset = 0);
    void CopyColumnToGpu(string colname);
    void CopyColumnToHost(string colname, size_t offset, size_t RecCount);
    void CopyColumnToHost(string colname);
    void CopyToHost(size_t offset, size_t count);
    float_type* get_float_type_by_name(string name);
    int_type* get_int_by_name(string name);
    float_type* get_host_float_by_name(string name);
    int_type* get_host_int_by_name(string name);
    void GroupBy(std::stack<string> columnRef);
    void addDeviceColumn(int_type* col, string colName, size_t recCount);
    void addDeviceColumn(float_type* col, string colName, size_t recCount, bool is_decimal);
    void compress(string file_name, size_t offset, unsigned int check_type, unsigned int check_val, size_t mCount);
    void writeHeader(string file_name, string colname, unsigned int tot_segs);
	void reWriteHeader(string file_name, string colname, unsigned int tot_segs, size_t newRecs, size_t maxRecs1);
    void writeSortHeader(string file_name);
    void Display(unsigned int limit, bool binary, bool term);
    void Store(string file_name, char* sep, unsigned int limit, bool binary, bool term = 0);
    void compress_char(string file_name, string colname, size_t mCount, size_t offset);
    bool LoadBigFile(FILE* file_p);
    void free();
    bool* logical_and(bool* column1, bool* column2);
    bool* logical_or(bool* column1, bool* column2);
    bool* compare(int_type s, int_type d, int_type op_type);
    bool* compare(float_type s, float_type d, int_type op_type);
    bool* compare(int_type* column1, int_type d, int_type op_type);
    bool* compare(float_type* column1, float_type d, int_type op_type);
    bool* compare(int_type* column1, int_type* column2, int_type op_type);
    bool* compare(float_type* column1, float_type* column2, int_type op_type);
    bool* compare(float_type* column1, int_type* column2, int_type op_type);
    float_type* op(int_type* column1, float_type* column2, string op_type, int reverse);
    int_type* op(int_type* column1, int_type* column2, string op_type, int reverse);
    float_type* op(float_type* column1, float_type* column2, string op_type, int reverse);
    int_type* op(int_type* column1, int_type d, string op_type, int reverse);
    float_type* op(int_type* column1, float_type d, string op_type, int reverse);
    float_type* op(float_type* column1, float_type d, string op_type,int reverse);
};
