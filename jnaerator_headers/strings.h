/// ======================================== H File =================================================
//---------------------------------------------------------------------------
#pragma once
#ifndef STRINGS_H
#define STRINGS_H
//---------------------------------------------------------------------------
#include "cm.h"
//---------------------------------------------------------------------------
/**
* JOIN on host static strings
* @param d_int - the input array of indices of joining strings.
* @param real_count - the input length of the input array indices.
* @param d - the input array of strings that will are joined.
* @param d_char - the output array of joined strings.
* @param len - the input length of string.
*/
void str_gather_host(unsigned int* d_int, size_t real_count, void* d, void* d_char, unsigned int len);

/**
* JOIN on device static strings
* @param d_int - the input array of indices of joining strings.
* @param real_count - the input length of the input array indices.
* @param d - the input array of strings that will are joined.
* @param d_char - the output array of joined strings.
* @param len - the input length of string.
*/
void str_gather(void* d_int, size_t real_count, void* d, void* d_char, const unsigned int len);

/**
*  SORT on host indices by static strings
* @param tmp - the input array of sorting strings.
* @param RecCount - the input length of the input array strings.
* @param permutation - the output array of sorted indices.
* @param desc_order - true - keys are sorted in descending order, false - keys are sorted in ascending order.
* @param len - the input length of string in bytes.
*/
void str_sort_host(char* tmp, size_t RecCount, unsigned int* permutation, bool desc_order, unsigned int len);

/**
*  SORT on device indices by static strings
* @param tmp - the input array of sorting strings.
* @param RecCount - the input length of the input array strings.
* @param permutation - the output array of sorted indices.
* @param desc_order - true - keys are sorted in descending order, false - keys are sorted in ascending order.
* @param len - the input length of string in bytes.
*/
void str_sort(char* tmp, size_t RecCount, unsigned int* permutation, bool desc_order, unsigned int len);

void str_merge_by_key1(long long int* keys,
					  char* values_first1, char* values_first2,
					  unsigned int len,
					  char* tmp, size_t offset1, size_t offset2);
//---------------------------------------------------------------------------
#endif	/// STRINGS_H
