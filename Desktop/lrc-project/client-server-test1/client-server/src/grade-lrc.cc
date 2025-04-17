#include"../include/grade-lrc.hh"
using namespace std;
using namespace ClientServer;


void  NewLRC:: new_lrc_generate_matrix (int k, int r, int p, int *final_matrix_g,int *final_matrix_l) {
    // Generate the original Cauchy matrix
    // 这里的k是数据块数，r是全局校验块数，p是局部校验块数
    // 这里的final_matrix是最终生成的矩阵
    int *x= new int[r];
    int *y= new int[k];
    for (int i = 0; i < r; i++)
    {
        x[i] = i;
    }
    for (int i = 0; i < k; i++)
    {
        y[i] = i+r;
    }
    final_matrix_g = cauchy_xy_coding_matrix(k, r, 8,x,y);

    //局部校验位系数,最后一个局部校验位由g_k+r+1-(alpha[i])得出
    int *local_matrix = new int[k*p];
    int numerator = 1;
    int denominator = 1;
    int *alpha = new int[k];
    int *beta = new int[k];
    int x_r_1 = r+1;


    // 计算分子：prod (b_{r+1} - b_s) for s=1 to r
    for (int s = 0; s < r; ++s) {
        numerator = galois_single_multiply(numerator, galois_single_divide(x_r_1 - x[s], 1, 8), 8);
    }

    // 计算分母：prod (a_i - b_t) for t=1 to r+1
    for (int i = 0; i < k; i++)
    {
        for (int t = 0; t < r+1; ++t) {
            denominator = galois_single_multiply(denominator, galois_single_divide(y[i] - x[t], 1, 8), 8);
        }
        alpha[i] = galois_single_divide(numerator, denominator, 8);
    }
    //计算最后一位局部校验位g_k+r+1，locality>=r
    for (int i = 0; i < k; i++)
    {
        beta[i] = galois_single_divide(1, (x_r_1^y[i]), 8);
    }
    
   
    // 计算局部校验位矩阵
    int local_group_size = (k+r)/p;
    int remainder = (k+r)%p;
    //p整除k+r
    if (remainder == 0)
    {
        for (int  i = 1; i < (p+1); i++){
            for (int j = 0; j < k; j++){
                if (i!= p){
                    if (j>=(i-1)*local_group_size&&j<i*local_group_size){
                    local_matrix[(i-1)*k+j] = alpha[i-1];
                    }else{
                    local_matrix[(i-1)*k+j] = 0;
                    }
                }else{
                    //局部校验位生成矩阵最后一行
                    local_matrix[(i-1)*k+j] = beta[j]^alpha[j];
                }
              
                
                }
                
        }
    }
 
    
}


void  NewLRC:: new_encode(int k, int r, int p, char **data_ptrs, char **global_coding_ptrs, char **local_coding_ptrs,int blocksize) {
    // Generate the new LRC matrix
    vector<int> new_matrix_g(k*r, 0);
    vector<int> new_matrix_l(k*p, 0);
    new_lrc_generate_matrix(k, r, p, new_matrix_g.data(), new_matrix_l.data());
    jerasure_matrix_encode(k, r, 8, new_matrix_g.data(), data_ptrs, global_coding_ptrs, blocksize); 
    jerasure_matrix_encode(k, p, 8, new_matrix_l.data(), data_ptrs, local_coding_ptrs, blocksize);
}       

int main() {
    // Example usage of Azure functions
    return 0;
}