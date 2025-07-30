'use client';

import { Card, CardContent, CardHeader, CardTitle } from '../ui/card';
import { Skeleton } from '../ui/skeleton';
import { ProcessedData } from '@/types';
import { Loader } from '@/components/ui/loader';

interface MetricsCardsProps {
  data: ProcessedData[];
  isLoading: boolean;
}

export function MetricsCards({ data, isLoading }: MetricsCardsProps) {
  if (isLoading) {
    return (
      <div className="w-full flex justify-center items-center py-12">
        <Loader text="Loading metrics..." size="lg" />
      </div>
    );
  }

  // Debug logging
  console.log('MetricsCards data:', data);
  console.log('Data length:', data?.length);
  console.log('Sample item:', data?.[0]);

  // Handle case where data is undefined or empty
  if (!data || data.length === 0) {
    return (
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader>
            <CardTitle>Total Engagement</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">No Data</div>
          </CardContent>
        </Card>
        
        <Card>
          <CardHeader>
            <CardTitle>Total Sales</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">No Data</div>
          </CardContent>
        </Card>
        
        <Card>
          <CardHeader>
            <CardTitle>Active Users</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">No Data</div>
          </CardContent>
        </Card>
        
        <Card>
          <CardHeader>
            <CardTitle>Avg Engagement</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">No Data</div>
          </CardContent>
        </Card>
      </div>
    );
  }

  // Ensure we're working with valid numbers
  const totalEngagement = data.reduce((sum, item) => {
    const likes = Number(item.likes) || 0;
    const comments = Number(item.comments) || 0;
    const shares = Number(item.shares) || 0;
    return sum + likes + comments + shares;
  }, 0);
  
  const totalSales = data.reduce((sum, item) => {
    const sales = Number(item.total_sales_attributed) || 0;
    return sum + sales;
  }, 0);
  
  const uniqueUsers = new Set(data.map(item => item.user_id).filter(id => id)).size;
  const avgEngagement = data.length > 0 ? totalEngagement / data.length : 0;

  // Debug the calculated values
  console.log('Calculated metrics:', {
    totalEngagement,
    totalSales,
    uniqueUsers,
    avgEngagement,
    dataLength: data.length
  });

  return (
    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
      <Card>
        <CardHeader>
          <CardTitle>Total Engagement</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{totalEngagement.toLocaleString()}</div>
          <p className="text-xs text-muted-foreground">From {data.length} posts</p>
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader>
          <CardTitle>Total Sales</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">${totalSales.toLocaleString()}</div>
          <p className="text-xs text-muted-foreground">Revenue attributed</p>
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader>
          <CardTitle>Active Users</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{uniqueUsers}</div>
          <p className="text-xs text-muted-foreground">Unique advocates</p>
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader>
          <CardTitle>Avg Engagement</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{Math.round(avgEngagement).toLocaleString()}</div>
          <p className="text-xs text-muted-foreground">Per post</p>
        </CardContent>
      </Card>
    </div>
  );
}