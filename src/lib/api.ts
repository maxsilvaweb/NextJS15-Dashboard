import axios from 'axios';
import { ProcessedData, PaginatedResponse } from '@/types';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL;

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const api = {
  getProcessedData: async (params: {
    page?: number;
    limit?: number;
    user_id?: string;
    platform?: string;
    program_id?: string;
  } = {}) => {
    const response = await apiClient.get<{data: ProcessedData[], total: number}>('/api/v1/processed-data', {
      params: {
        page: params.page || 1,
        limit: params.limit || 10,
        user_id: params.user_id,
        platform: params.platform,
        program_id: params.program_id,
      },
    });
    
    const responseData = response.data;
    
    return {
      data: responseData.data || [],
      total: responseData.total || 0, // Use the total from API response
      page: params.page || 1,
      limit: params.limit || 10,
      totalPages: Math.ceil((responseData.total || 0) / (params.limit || 10)), // Calculate using total, not current page length
    } as PaginatedResponse<ProcessedData>;
  },

  getHealthCheck: async () => {
    const response = await apiClient.get('/health');
    return response.data;
  },
};