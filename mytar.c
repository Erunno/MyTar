#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#define FILE_BUFFER_SIZE 1048576 // 1MB
#define TAR_REC_SIZE (sizeof (struct tar_record))
#define CHAR_SIZE sizeof(char)
struct arg {
    char* value;
    int handled;
};

struct tar_record {
    char name[100];
    char unused_props_1[24];
    char size_string[12];
    char unused_props_2[20];
    char typeflag;
    char unused_props_3[100];
    char magic[6];
    char unused_props_4[249];
};

struct tar_state {
    char* archive_name;
    char* archive_buffer;
    struct tar_record* buff_end;    

    char** files_from_args;
    int files_from_args_count;
    char** found_files;
    size_t found_files_count;

    int handle_all_files;

    int f_is_set;
    int t_is_set;
    int x_is_set;
    int v_is_set;

    int argc;
    struct arg* argv;

    int raw_argc;
    char** raw_argv;

    int return_code;

    size_t file_size;
    size_t current_file_block;
    size_t file_block_count;
    FILE* fp;
};

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

void err(int code, char* err_msg) {
    printf("%s\n", err_msg);
    exit(code);
}

void errf(int code, char* err_msg, char ch) {
    printf(err_msg, ch);
    exit(code);
}

void assert_memory(void* ptr) {
    if (ptr == NULL)
        err(2, "out of memory");
}

void assert_eq(size_t a, size_t b, char* msg) {
    if (a != b)
        err(2, msg); 
}

void assert_not_zero(size_t a, char* msg) {
    if (a == 0)
        err(2, msg); 
}

#define str_eq(s1, s2) (strcmp(s1, s2) == 0)

char* get_str_copy(char* s) {
    size_t size = strlen(s);

    char* cp = malloc(size);
    assert_memory(cp);

    strcpy(cp, s);

    return cp;
}

int to_int(char* str) {
    int num = 0;

    while(*str != '\0') {
        num = num * 8 + (*str - '0');
        ++str;
    }

    return num;
}

#define ceiling_div(a, div_b) ((a + div_b - 1) / div_b)

void asserted_fseek(FILE* fp, long offset, int origin) {
    int retval = fseek(fp, offset, origin);
    
    if (retval != 0)
        err(2, "fseek has crashed");
}

size_t get_file_size(FILE *fp) {
    asserted_fseek(fp, 0, SEEK_END);
    size_t size = ftell(fp);
    asserted_fseek(fp, 0, SEEK_SET);

    return size;
}

static int last_block_loaded(struct tar_state* this) {
    return this->current_file_block == this->file_block_count - 1;
}

static void load_next_block_from_file(struct tar_state* this) {
    this->current_file_block++;
    size_t file_position = this->current_file_block * FILE_BUFFER_SIZE;

    asserted_fseek(this->fp, file_position, SEEK_SET);
    size_t bytes_read = fread(this->archive_buffer, CHAR_SIZE, FILE_BUFFER_SIZE, this->fp);
    
    assert_not_zero(bytes_read, "unable to read next block from archive file");

    if(last_block_loaded(this)) {
        size_t valid_bytes_count = this->file_size % FILE_BUFFER_SIZE;
        this->buff_end = (struct tar_record*)(this->archive_buffer + valid_bytes_count);
    }
}

static struct tar_record* move_to_next_tar_record(struct tar_state* this, struct tar_record* iter) {

    if (++iter != this->buff_end || last_block_loaded(this)) 
        return iter;
    
    load_next_block_from_file(this);
    return (struct tar_record*)this->archive_buffer;
}

static void init_buffer_with_file(struct tar_state* this) {

	if ((this->fp = fopen(this->archive_name, "r")) == NULL)
		err(2, "error: cannot open specified archive");

    this->file_size = get_file_size(this->fp);
    this->archive_buffer = malloc(FILE_BUFFER_SIZE);
    assert_memory(this->archive_buffer);

    this->current_file_block = -1;
    this->file_block_count = (size_t)ceiling_div(this->file_size, FILE_BUFFER_SIZE);

    this->buff_end = (struct tar_record*)(this->archive_buffer + FILE_BUFFER_SIZE);

    load_next_block_from_file(this);
}

static int rec_is_null(const struct tar_record* rec) {
    char* c = (char*)rec;

    for (size_t i = 0; i < TAR_REC_SIZE; i++)
        if (c[i] != 0)
            return 0;

    return 1;
}

static int is_wanted_file(struct tar_state* this, const char* fname) {
    if (this->files_from_args_count == 0)
        return 1;

    for (int i = 0; i < this->files_from_args_count; i++)
        if (str_eq(this->files_from_args[i], fname))
            return 1;

    return 0;
}

static struct tar_record* move_to_next_tar_header(struct tar_state* this, struct tar_record* iter) {
    int size = to_int(iter->size_string);
    size_t num_of_recs_to_skip = (size_t)ceiling_div(size, TAR_REC_SIZE) + 1;
    
    for(size_t i = 0; i < num_of_recs_to_skip; i++)
        iter = move_to_next_tar_record(this, iter);

    return iter; 
}

static int file_found(struct tar_state* this, char* fname) {
    for (size_t i = 0; i < this->found_files_count; i++)
        if (str_eq(fname, this->found_files[i]))
            return 1;

    return 0;
}

static void print_not_found_files(struct tar_state* this) {
    for (int i = 0; i < this->files_from_args_count; i++) {
        char* fname = this->files_from_args[i];
        
        if (!file_found(this, fname)) {
            printf("mytar: %s: Not found in archive\n", fname);
            this->return_code = 2;
        }
    }
}

static int is_on_end_of_buffer(struct tar_state* this, struct tar_record* curr_rec) {
    return 
        rec_is_null(curr_rec) ||
        (last_block_loaded(this) && curr_rec >= this->buff_end);
}

static void assert_archive_consistency(struct tar_state* this, struct tar_record* final_rec) {
    if (final_rec == this->buff_end) // two missing zero block
        return;

    if (final_rec + 1 == this->buff_end) // one missing zero block
        printf("mytar: A lone zero block at 22\n");

    if (final_rec + 1 > this->buff_end) // trunked archive 
        err(2, "mytar: Unexpected EOF in archive\nmytar: Error is not recoverable: exiting now\n"); 
}

static void assert_valid_header(struct tar_record* rec) {

    if (str_eq(rec->magic, "ustar") || rec->typeflag < '0')
        err(2, "mytar: This does not look like a tar archive\nmytar: Exiting with failure status due to previous errors");

    if (rec->typeflag != '0')
        errf(2, "mytar: Unsupported header type: %d\n", rec->typeflag);
}

static void write_to_file(struct tar_state* this, FILE* out_fp, struct tar_record* from, size_t byte_count) {
    char* buffer = malloc(FILE_BUFFER_SIZE);
    assert_memory(buffer);

    size_t offset_from_start_of_curr_block = (char*)from - this->archive_buffer;

    size_t start_position = 
        this->current_file_block * FILE_BUFFER_SIZE + 
        offset_from_start_of_curr_block;

    size_t full_blocks_count = byte_count / FILE_BUFFER_SIZE;

    for (size_t i = 0; i < full_blocks_count + 1; i++) {
        size_t tar_file_position = start_position + i * FILE_BUFFER_SIZE;
        size_t out_file_position = i * FILE_BUFFER_SIZE;

        size_t valid_buff_size = i < full_blocks_count
            ? FILE_BUFFER_SIZE
            : byte_count % FILE_BUFFER_SIZE;

        asserted_fseek(this->fp, tar_file_position, SEEK_SET);
        size_t bytes_read = fread(buffer, CHAR_SIZE, valid_buff_size, this->fp);
        assert_eq(bytes_read, valid_buff_size, "unable to read from archive file");

        asserted_fseek(out_fp, out_file_position, SEEK_SET);
        size_t bytes_wrote = fwrite(buffer, CHAR_SIZE, valid_buff_size, out_fp);
        assert_eq(bytes_wrote, valid_buff_size, "unable to write to output file");
    }

    free(buffer);
    fclose(out_fp);
}

static void extract_file(struct tar_state* this, struct tar_record* header) { 
	FILE *fp;
    
    if ((fp = fopen(header->name, "w")) == NULL)
        err(2, "error: cannot write to file");

    struct tar_record* virtual_buffer_end 
        = (struct tar_record*)(this->archive_buffer + this->file_size);    

    int fsize = to_int(header->size_string);
    struct tar_record* expected_end = header 
        + (fsize / TAR_REC_SIZE + 1 + (fsize % TAR_REC_SIZE != 0));

    int fsize_in_buffer = expected_end > virtual_buffer_end 
        ? fsize - (int)(expected_end - virtual_buffer_end) * (int)TAR_REC_SIZE
        : fsize;

    write_to_file(this, fp, header + 1, fsize_in_buffer);

    if (expected_end > virtual_buffer_end)
        err(2, "mytar: Unexpected EOF in archive\nmytar: Error is not recoverable: exiting now");
}

static void t_handle_file(struct tar_record* rec) {
    printf("%s\n", rec->name);
}

static void x_handle_file(struct tar_state* this, struct tar_record* rec) {

    if (this->v_is_set) 
        printf("%s\n", rec->name);

    extract_file(this, rec);
}

static void handle_archive(struct tar_state* this) {
    struct tar_record* curr_rec = (struct tar_record*)(this->archive_buffer);
    
    while(!is_on_end_of_buffer(this, curr_rec)) {
        
        assert_valid_header(curr_rec);

        if (is_wanted_file(this, (char*)curr_rec->name)) {
            
            if (this->t_is_set) 
                t_handle_file(curr_rec);
            else if (this->x_is_set)
                x_handle_file(this, curr_rec);
                       
            if (!this->handle_all_files)
                this->found_files[this->found_files_count++] = get_str_copy(curr_rec->name);
        }

        curr_rec = move_to_next_tar_header(this, curr_rec);
    }

    if (!this->handle_all_files)
        print_not_found_files(this);

    assert_archive_consistency(this, curr_rec);
}

#define NO_OPT_FOUNT_IDX -1

static int find_option_loc(struct tar_state* this, char* option_string) {
    for (int i = 0; i < this->argc; i++) {
        char* curr_str = this->argv[i].value;
        if (str_eq(curr_str, option_string))
            return i;
    }
    return NO_OPT_FOUNT_IDX;
}

static void parse_opt_x(struct tar_state* this) {
    int opt_idx = find_option_loc(this, "-x");
    
    if (opt_idx == NO_OPT_FOUNT_IDX)
        return;

    this->x_is_set = 1;
    this->argv[opt_idx].handled = 1;

    this->handle_all_files = this->files_from_args_count == 0;
}

static void parse_opt_f(struct tar_state* this) {
    
    int opt_idx = find_option_loc(this, "-f");
    
    if (opt_idx == NO_OPT_FOUNT_IDX ||
        opt_idx + 1 == this->argc) // no archive name after -f
        return;

    this->f_is_set = 1;
    this->archive_name = this->argv[opt_idx + 1].value;

    this->argv[opt_idx].handled = 1;
    this->argv[opt_idx + 1].handled = 1;
}

static int is_not_opt(char* arg) {
    return arg[0] != '-';
}

static void prepare_unused_files_from_args(struct tar_state* this) {
    
    int next_file_idx = 0;

    for (int i = 0; i < this->argc; i++) {
        struct arg curr_arg = this->argv[i]; 

        if (!curr_arg.handled && is_not_opt(curr_arg.value)) {
            this->files_from_args[next_file_idx] = curr_arg.value;
            next_file_idx++;
        }
    }

    this->files_from_args_count = next_file_idx;
}

static void parse_opt_t(struct tar_state* this) {
    int opt_idx = find_option_loc(this, "-t");
    
    if (opt_idx == NO_OPT_FOUNT_IDX)
        return;

    this->t_is_set = 1;
    this->argv[opt_idx].handled = 1;

    this->handle_all_files = this->files_from_args_count == 0;
}

static void parse_opt_v(struct tar_state* this) {
    int opt_idx = find_option_loc(this, "-v");
    
    if (opt_idx == NO_OPT_FOUNT_IDX)
        return;

    this->v_is_set = 1;
    this->argv[opt_idx].handled = 1;
}

static void assert_valid_args(struct tar_state* this) {
    if(!this->f_is_set) 
        err(2, "error: archive not specified (see -f)");
    if(!this->t_is_set && !this->x_is_set) 
        err(2, "error: action not specified");
} 

static void parse_args(struct tar_state* this) {
    parse_opt_f(this);
    parse_opt_v(this);

    prepare_unused_files_from_args(this);

    parse_opt_t(this);
    parse_opt_x(this);
} 

static void prepare_args(struct tar_state* this) {
    this->argc = this->raw_argc - 1;
    this->argv = malloc(sizeof(struct arg) * this->argc);
    assert_memory(this->argv);

    for (int i = 1; i < this->raw_argc; i++) {
        this->argv[i - 1].value = this->raw_argv[i];
        this->argv[i - 1].handled = 0;
    }

    this->found_files = malloc(sizeof(char*) * this->argc);
    this->files_from_args = malloc(sizeof(char*) * this->argc);

    assert_memory(this->found_files);
    assert_memory(this->files_from_args);
}

static void dispose_resources(struct tar_state* this) {
    if (this->argv != NULL)
        free(this->argv);

    if (this->found_files != NULL) {
        for(size_t i = 0; i < this->found_files_count; i++)
            free(this->found_files[i]);

        free(this->found_files);
    }

    if (this->files_from_args != NULL)
        free(this->files_from_args);

    if (this->archive_buffer != NULL)
        free(this->archive_buffer);

    fclose(this->fp);
}

void run_tar(struct tar_state* this) {
    prepare_args(this);
    parse_args(this);
    assert_valid_args(this);

    init_buffer_with_file(this);

    handle_archive(this);

    dispose_resources(this);
}


int main(int argc, char** argv) {
    
    struct tar_state tar_state = {
        .raw_argc = argc,
        .raw_argv = argv,

        .f_is_set = 0,
        .t_is_set = 0,
        .found_files_count = 0,
        .handle_all_files = 0,

        .files_from_args = NULL,
        .files_from_args_count = 0,
        
        .return_code = 0,

        .file_size = 0,
        .current_file_block = 0,
    };
    
    run_tar(&tar_state);

    if (tar_state.return_code != 0) {
        err(
            tar_state.return_code,
            "mytar: Exiting with failure status due to previous errors\n" 
        );
    }
}
