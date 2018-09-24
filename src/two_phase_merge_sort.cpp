#include<iostream>
#include <fstream>
#include <iterator>
#include <sstream>

#include<string>
#include<vector>
#include<algorithm>
using namespace std;

string metafile_path("../files/metadata.txt");
string inter_outpath("../files/inter_out.txt");

string input_path;
string output_path;
int num_cols;
vector<string> col_names;
vector<int> col_lengths;
vector<int> sort_col_idx;
long long MM; // main memory size
long long R; // record size
long long MAXN; // number of record that can be brought to main memory at once
long long NUMR;
bool ASC;

struct Record
{
    vector<string> fields;
    Record(vector<string> vs)
    {
        fields = vs;
    }
};

bool mycompare(Record & r1, Record & r2)
{
    for(int i = 0; i < sort_col_idx.size(); i++)
    {
        if(r1.fields[sort_col_idx[i]] != r2.fields[sort_col_idx[i]])
            return (r1.fields[sort_col_idx[i]] < r2.fields[sort_col_idx[i]]);
    }
    return true;
}
void phase1sort(vector<Record> & records)
{
    sort(records.begin(), records.end(), mycompare);
}

void preprocess(int argc, char ** argv)
{
    // read meta file
    ifstream metafile(metafile_path);
    if(! metafile.good())
    {
        cerr << "Error in metafile" << endl;
        exit(-1);
    }
    string line;
    num_cols = 0;
    while(metafile >> line)
    {
        int comma = line.find(",");
        string a = line.substr(0, comma);
        string b = line.substr(comma+1);
        col_names.push_back(a);
        col_lengths.push_back(stoi(b));
        num_cols++;
    }
    metafile.close();

    if(argc < 6)
    {
        cerr << "Not enough args" << endl;
        cerr << "Usage : " << argv[0] << " <input_path> <output_path> <main_memory_space_MB> <asc/desc> <c0> [<c1> ..]" << endl;
        exit(-1);
    }

    input_path = argv[1];
    output_path = argv[2];

    // main memory size
    MM = stol(argv[3]) * 1000000; // megabytes to bytes

    // sort order : asc or desc
    string order = argv[4];
    if(order == "asc") ASC = true;
    else if(order == "desc") ASC = false;
    else
    {
        cerr << "order can be asc or desc" << endl;
        exit(-1);
    }

    // sort key columns
    for(int i = 5; i < argc; i++)
    {
        string col(argv[i]);
        auto it = find(col_names.begin(), col_names.end(), col);
        if(it == col_names.end())
        {
            cerr << "invalid sort key column names" << endl;
            exit(-1);
        }
        sort_col_idx.push_back(int(it - col_names.begin()));
    }

    // decide record size and number of records
    ifstream inpf(input_path);
    if(!inpf.good())
    {
        cerr << "Unable to read input file" << endl;
        exit(-1);
    }
    inpf.seekg(0, inpf.end);
    long long file_size = inpf.tellg();
    inpf.seekg(0, inpf.beg);

    inpf.unsetf(ios_base::skipws);
    long long line_count = count(istream_iterator<char>(inpf), istream_iterator<char>(), '\n');
    inpf.close();

    if(file_size % line_count != 0)
    {
        cerr << "File size is not a multiple of each line size" << endl;
        exit(-1);
    }
    NUMR = line_count;
    R = file_size / line_count;

    // ideally it should be (MM/R) but mergesort needs double space to sort
    // and there is other constant space requirement for this code
    // so I am taking (MM/(3R))
    MAXN = MM / (3*R);
    MAXN = 15;


    cout << "file_size : " << file_size << endl;
    cout << "R : " << R << endl;
    cout << "NUMR : " << NUMR << endl;
    cout << "MAXN : " << MAXN << endl;
    cout << endl << endl;
    // for(int i = 0; i < col_names.size(); i++)
    //     cout << col_names[i] << " " << col_lengths[i] << endl;
    // for(int i = 0; i < sort_col_idx.size(); i++)
    //     cout << sort_col_idx[i] << endl;
}

void write_records(ofstream & of, vector<Record> & records)
{
    string s;
    string delim(" ");
    for(int i = 0; i < records.size(); i++)
    {
        s = records[i].fields[0];
        for(int j = 1; j < num_cols; j++)
            s = s + "  " + records[i].fields[j];
        s = s + "\n";
        of << s;
        // of << s << endl;
    }
}

void phase_one()
{
    ifstream inpf(input_path);
    if(!inpf.good())
    {
        cerr << "Unable to open input file" << endl;
        exit(-1);
    }

    ofstream inter_outf(inter_outpath);
    if(!inter_outf.good())
    {
        cerr << "Unable to open inter out file" << endl;
        exit(-1);
    }

    long long idx=0;
    string line, tok;
    vector<Record> records;
    while(getline(inpf, line))
    {
        vector<string> vs(num_cols);
        long long pos = 0;
        for(int i = 0; i < num_cols; i++)
        {
            long long off = line.find_first_not_of(" ", pos);
            vs[i] = line.substr(off, col_lengths[i]);
            pos = off + col_lengths[i];
        }
        records.push_back(Record(vs));
        idx++;

        if((idx % MAXN == 0) || idx == NUMR)
        {
            phase1sort(records);
            // for(int i = 0; i < records.size(); i++)
            // {
            //     for(int j = 0; j < num_cols; j++)
            //         cout << records[i].fields[j] << " ";
            //     cout << endl;
            // }
            // cout << endl << endl;
            write_records(inter_outf, records);
            records.clear();
        }
    }
    inpf.close();
    inter_outf.close();
}

int main(int argc, char **argv)
{
    preprocess(argc, argv);
    phase_one();
    return 0;
}
