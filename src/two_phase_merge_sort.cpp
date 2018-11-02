#include<iostream>
#include <fstream>
#include <iterator>
#include <sstream>

#include<string>
#include<vector>
#include<algorithm>
#include <functional>
#include <queue>

using namespace std;

string metafile_path("../files/metadata.txt");
string inter_outdir("../files/inter_files/");

string input_path;
string output_path;
int num_cols;
vector<string> col_names;
vector<int> col_lengths;
vector<int> sort_col_idx;
long long MM; // main memory size
long long R; // record size
long long NUMR; // total number of records
long long NUMSUB; // number of sublists
vector<long long> SUBLEN;
bool ASC;

struct Record
{
    vector<string> fields;
    Record(vector<string> vs)
    {
        fields = vs;
    }
};


bool compare_phase1(Record & r1, Record & r2)
{
    if(ASC)
    {
        for(int i = 0; i < sort_col_idx.size(); i++)
        {
            if(r1.fields[sort_col_idx[i]] != r2.fields[sort_col_idx[i]])
                return (r1.fields[sort_col_idx[i]] < r2.fields[sort_col_idx[i]]);
        }
    }
    else
    {
        for(int i = 0; i < sort_col_idx.size(); i++)
        {
            if(r1.fields[sort_col_idx[i]] != r2.fields[sort_col_idx[i]])
                return (r1.fields[sort_col_idx[i]] > r2.fields[sort_col_idx[i]]);
        }
    }
    return false;
}

void phase1sort(vector<Record> & records)
{
    sort(records.begin(), records.end(), compare_phase1);
}

struct compare_phase2
 {
   bool operator()(const pair<Record, int> & r1, const pair<Record, int> & r2)
   {
       if(ASC)
       {
           for(int i = 0; i < sort_col_idx.size(); i++)
           {
               if(r1.first.fields[sort_col_idx[i]] != r2.first.fields[sort_col_idx[i]])
                   return (r1.first.fields[sort_col_idx[i]] > r2.first.fields[sort_col_idx[i]]);
           }
       }
       else
       {
           for(int i = 0; i < sort_col_idx.size(); i++)
           {
               if(r1.first.fields[sort_col_idx[i]] != r2.first.fields[sort_col_idx[i]])
                   return (r1.first.fields[sort_col_idx[i]] < r2.first.fields[sort_col_idx[i]]);
           }
       }
       return r1.second > r2.second;
   }
 };

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

    // get record size from meta file
    // make sure to take care of delimeter size after each record and
    // ending of each record(either '\n' or '\r\n')
    long long meta_rsz = 0;
    for(int i = 0; i < col_lengths.size(); i++)
        meta_rsz += col_lengths[i];
    meta_rsz += col_lengths.size()*2;

    if(argc < 6)
    {
        cerr << "Not enough args" << endl;
        cerr << "Usage : " << argv[0] << " <input_path> <output_path> <main_memory_space_MB> <asc/desc> <c0> [<c1> ..]" << endl;
        exit(-1);
    }

    input_path = argv[1];
    output_path = argv[2];

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

    // inpf.unsetf(ios_base::skipws);
    // long long line_count = count(istream_iterator<char>(inpf), istream_iterator<char>(), '\n');
    // inpf.close();
    // if(file_size % line_count != 0)
    // {
    //     cerr << "File size is not a multiple of each line size" << endl;
    //     exit(-1);
    // }

    if(file_size % meta_rsz != 0)
    {
        cerr << "File size is not a multiple of each record size" << endl;
        exit(-1);
    }
    long long line_count = file_size / meta_rsz;

    NUMR = line_count;
    R = file_size / line_count;

    // main memory capacity in terms of number of records
    MM = stol(argv[3]);
    MM = MM / R;

    cout << "file_size : " << file_size << endl;
    cout << "R : " << R << endl;
    cout << "MM : " << MM << endl;
    cout << "NUMR : " << NUMR << endl;
    cout << endl;

    if(MM * MM < NUMR)
    {
        cerr << "Too many records for two way merge sort, need K-way(K > 2) merge sort" << endl;
        exit(-1);
    }
    // for(int i = 0; i < col_names.size(); i++)
    //     cout << col_names[i] << " " << col_lengths[i] << endl;
    // for(int i = 0; i < sort_col_idx.size(); i++)
    //     cout << sort_col_idx[i] << endl;
}

Record construct_record(string & line)
{
    vector<string> vs(num_cols);
    long long pos = 0, off = 0;
    for(int i = 0; i < num_cols; i++)
    {
        vs[i] = line.substr(off, col_lengths[i]);
        off = line.find_first_not_of(" ", off + col_lengths[i]);
    }
    return Record(vs);
}

void write_records(ofstream & of, vector<Record> & records)
{
    string s;
    for(int i = 0; i < records.size(); i++)
    {
        s = records[i].fields[0];
        for(int j = 1; j < num_cols; j++)
            s = s + "  " + records[i].fields[j];
        s = s + "\r\n";
        of << s;
        // cout << s;
        // of << s << endl;
    }
    of.flush();
}

void write_records_chunk(ofstream & of, vector<Record> & records)
{
    string s;
    vector<string> vs(records.size());
    int idx = records.size()-1;
    while(!records.empty())
    {
        Record & r = records.back();
        s = r.fields[0];
        for(int j = 1; j < num_cols; j++)
            s = s + "  " + r.fields[j];
        // s = s + "\r\n";
        vs[idx] = s;
        idx--;
        records.pop_back();
    }
    std::stringstream fin;
    copy(vs.begin(), vs.end(), ostream_iterator<string>(fin, "\r\n"));
    of << fin.rdbuf();
    of.flush();
}

void read_records_chunk(ifstream & ifs, long long n, vector<Record> & records)
{
    char * buf = new char[n*R+1];
    ifs.read(buf, n*R);
    buf[n*R] = '\0';
    // cout << buf << endl;

    stringstream ss(buf);
    string line;
    while(getline(ss, line))
    {
        // cout << line << endl;
        records.push_back(construct_record(line));
    }
    delete buf;
}


void phase_one()
{
    long long MAXN = MM;
    cout << "Phase 1 started" << endl;
    cout << "MAXN : " << MAXN << endl;

    ifstream inpf(input_path);
    if(!inpf.good())
    {
        cerr << "Unable to open input file" << endl;
        exit(-1);
    }

    long long idx=0;
    NUMSUB = 0;
    string line, tok;
    vector<Record> records;
    while(getline(inpf, line))
    {
        Record r = construct_record(line);
        records.push_back(r);
        idx++;

        if((idx % MAXN == 0) || idx == NUMR)
        {
            phase1sort(records);
            string inter_outpath = inter_outdir + to_string(NUMSUB) + ".txt";
            ofstream inter_outf(inter_outpath);
            if(!inter_outf.good())
            {
                cerr << "Unable to open inter out file : " << inter_outpath<< endl;
                exit(-1);
            }
            write_records(inter_outf, records);
            inter_outf.close();
            records.clear();
            NUMSUB++;
        }
    }
    inpf.close();
    cout << "NUMSUB : " << NUMSUB << endl;
    cout << endl;
}


void phase_one_chunk()
{
    long long MAXN = MM;
    cout << "Phase 1 started" << endl;
    cout << "MAXN : " << MAXN << endl;

    ifstream inpf(input_path);
    if(!inpf.good())
    {
        cerr << "Unable to open input file" << endl;
        exit(-1);
    }

    NUMSUB = 0;
    while(true)
    {
        long long sz = min(MAXN, NUMR-NUMSUB*MAXN);
        if(sz <= 0) break;
        vector<Record> records;
        read_records_chunk(inpf, sz, records);

        phase1sort(records);
        string inter_outpath = inter_outdir + to_string(NUMSUB) + ".txt";
        ofstream inter_outf(inter_outpath);
        if(!inter_outf.good())
        {
            cerr << "Unable to open inter out file : " << inter_outpath<< endl;
            exit(-1);
        }
        SUBLEN.push_back(records.size());
        write_records_chunk(inter_outf, records);
        inter_outf.close();
        records.clear();
        NUMSUB++;
    }

    inpf.close();
    cout << "NUMSUB : " << NUMSUB << endl;
    cout << endl;
}


void phase_two()
{
    cout << "Phase 2 started" << endl;
    long long H = (long long)(MM / (1 + sqrt(NUMSUB)));
    long long SUBSZ = (MM - H) / NUMSUB;


    cout << "NUMSUB : " << NUMSUB << endl;
    cout << "SUBSZ : " << SUBSZ << endl;
    cout << "H : " << H << endl;

    ofstream outf(output_path);
    if(!outf.good())
    {
        cerr << "Unable to open output file" << endl;
        exit(-1);
    }
    ifstream inter_outf[NUMSUB];
    for(int i = 0; i < NUMSUB; i++)
    {
        string inter_outpath = inter_outdir + to_string(i) + ".txt";
        inter_outf[i].open(inter_outpath);
        if(!inter_outf[i].good())
        {
            cerr << "Unable to open inter out file : " << inter_outpath << endl;
            exit(-1);
        }
    }

    vector<Record> subl[NUMSUB];
    string line;
    for(int i = 0; i < NUMSUB; i++)
    {
        int j = 0;
        while((j < SUBSZ) && (getline(inter_outf[i], line)))
        {
            subl[i].push_back(construct_record(line));
            j++;
        }
    }
    vector<long long> sublidx(NUMSUB, 0);

    priority_queue<pair<Record, int>, vector<pair<Record, int> >, compare_phase2> pq;

    for(int i = 0; i < NUMSUB; i++)
        for(int j = 0; j < H/NUMSUB && j < subl[i].size(); j++)
        {
            pq.push({subl[i][j], i});
            sublidx[i]++;
        }

    vector<Record> out_records;
    while(!pq.empty())
    {
        pair<Record, int> ri = pq.top(); pq.pop();
        out_records.push_back(ri.first);

        if(out_records.size() >= H)
        {
            write_records(outf, out_records);
            out_records.clear();
        }

        int si = ri.second;
        if(sublidx[si] == subl[si].size())
        {
            subl[si].clear();
            int j = 0;
            while((j < SUBSZ) && getline(inter_outf[si], line))
            {
                subl[si].push_back(construct_record(line));
                j++;
            }

            sublidx[si] = 0;
            for(int k = 0; k < H/NUMSUB && k < subl[si].size(); k++)
            {
                pq.push({subl[si][k], si});
                sublidx[si]++;
            }
        }
        else
        {
            pq.push({subl[si][sublidx[si]], si});
            sublidx[si]++;
        }
    }
    if(out_records.size())
    {
        write_records(outf, out_records);
        out_records.clear();
    }
    for(int i = 0; i < NUMSUB; i++)
        inter_outf[i].close();
    outf.close();
    cout << endl;
}


void phase_two_chunk()
{
    cout << "Phase 2 started" << endl;
    // long long H = (long long)(MM / (1 + sqrt(NUMSUB)));
    long long H = MM / (1 + NUMSUB);
    long long SUBSZ = (MM - H) / NUMSUB;


    cout << "NUMSUB : " << NUMSUB << endl;
    cout << "SUBSZ : " << SUBSZ << endl;
    cout << "H : " << H << endl;

    ofstream outf(output_path);
    if(!outf.good())
    {
        cerr << "Unable to open output file" << endl;
        exit(-1);
    }
    ifstream inter_outf[NUMSUB];
    for(int i = 0; i < NUMSUB; i++)
    {
        string inter_outpath = inter_outdir + to_string(i) + ".txt";
        inter_outf[i].open(inter_outpath);
        if(!inter_outf[i].good())
        {
            cerr << "Unable to open inter out file : " << inter_outpath << endl;
            exit(-1);
        }
    }

    vector<Record> subl[NUMSUB];
    vector<long long> readidx(NUMSUB, 0LL);
    string line;
    for(int i = 0; i < NUMSUB; i++)
    {
        long long sz = min(SUBSZ, SUBLEN[i] - readidx[i]);
        read_records_chunk(inter_outf[i], sz, subl[i]);
        readidx[i] += sz;
    }
    vector<long long> sublidx(NUMSUB, 0);

    priority_queue<pair<Record, int>, vector<pair<Record, int> >, compare_phase2> pq;

    for(int i = 0; i < NUMSUB; i++)
        for(int j = 0; j < H/NUMSUB && j < subl[i].size(); j++)
        {
            pq.push({subl[i][j], i});
            sublidx[i]++;
        }

    vector<Record> out_records;
    while(!pq.empty())
    {
        pair<Record, int> ri = pq.top(); pq.pop();
        out_records.push_back(ri.first);

        if(out_records.size() >= H)
        {
            write_records_chunk(outf, out_records);
            out_records.clear();
        }

        int si = ri.second;
        if(sublidx[si] == subl[si].size())
        {
            long long sz = min(SUBSZ, SUBLEN[si] - readidx[si]);
            if(sz)
            {
                subl[si].clear();
                read_records_chunk(inter_outf[si], sz, subl[si]);
                readidx[si] += sz;

                sublidx[si] = 0;
                pq.push({subl[si][0], si});
                sublidx[si]++;
            }
        }
        else
        {
            pq.push({subl[si][sublidx[si]], si});
            sublidx[si]++;
        }
    }
    if(out_records.size())
    {
        write_records_chunk(outf, out_records);
        out_records.clear();
    }
    for(int i = 0; i < NUMSUB; i++)
        inter_outf[i].close();
    outf.close();
    cout << endl;
}



int main(int argc, char **argv)
{
    preprocess(argc, argv);
    // phase_one();
    phase_one_chunk();

    // phase_two();
    phase_two_chunk();
    return 0;
}
