export interface ProcessedData {
  id: number;
  user_id: string;
  name: string;
  email: string;
  instagram_handle?: string;
  tiktok_handle?: string;
  program_id: string;
  brand: string;
  task_id: string;
  platform: 'instagram' | 'tiktok';
  post_url: string;
  likes: number;
  comments: number;
  shares: number;
  reach: number;
  total_sales_attributed: number;
  source_file: string;
  created_at: string;
}

export interface PaginatedResponse<T> {
  data: T[];
  total: number;
  page: number;
  limit: number;
  totalPages: number;
}

export interface MetricsSummary {
  totalEngagement: number;
  totalSales: number;
  totalUsers: number;
  avgEngagementRate: number;
  platformBreakdown: { platform: string; count: number; engagement: number }[];
}