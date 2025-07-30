'use client';

import { useState } from 'react';
import useSWR from 'swr';
import { MetricsCards } from '@/components/dashboard/metrics-cards';
import { Charts } from '@/components/dashboard/charts';
import { DataTable } from '@/components/dashboard/data-table';
import { ProcessedData, PaginatedResponse } from '@/types';

const fetcher = (url: string) => fetch(url).then((res) => res.json());

export default function DashboardPage() {
  const [currentPage, setCurrentPage] = useState(1);
  const [limit, setLimit] = useState(20);

  const {
    data,
    error,
    isLoading,
  } = useSWR<PaginatedResponse<ProcessedData>>(
    `/api/processed-data?page=${currentPage}&limit=${limit}`,
    fetcher
  );

  const handlePageChange = (page: number) => {
    setCurrentPage(page);
  };

  const handleLimitChange = (newLimit: number) => {
    setLimit(newLimit);
    setCurrentPage(1);
  };

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-semibold text-foreground mb-2">Something went wrong</h2>
          <p className="text-muted-foreground">Failed to load dashboard data</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <div className="border-b">
        <div className="container mx-auto px-4 py-8">
          <div className="text-center space-y-2">
            <h1 className="text-4xl font-bold tracking-tight text-foreground">
              Dashboard
            </h1>
            <p className="text-lg text-muted-foreground max-w-2xl mx-auto">
              Monitor your performance and analytics with real-time insights
            </p>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="container mx-auto px-4 py-8">
        <div className="max-w-7xl mx-auto space-y-8">
          {/* Metrics Cards */}
          <div>
            <MetricsCards data={data?.data || []} isLoading={isLoading} />
          </div>
          
          {/* Analytics Chart */}
          <div className="bg-card rounded-lg border shadow-sm">
            <div className="p-6">
              {!isLoading && (
                <div className="text-center space-y-2 mb-6">
                  <h2 className="text-2xl font-semibold text-card-foreground">
                    Analytics Overview
                  </h2>
                  <p className="text-muted-foreground">
                    Monthly performance trends
                  </p>
                </div>
              )}
              <Charts data={data?.data || []} />
            </div>
          </div>
          
          {/* Data Table */}
          <div className="bg-card rounded-lg border shadow-sm">
            {!isLoading && (
              <div className="p-6 border-b">
                <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
                  <div>
                    <h2 className="text-2xl font-semibold text-card-foreground">
                      Recent Activity
                    </h2>
                    <p className="text-muted-foreground mt-1">
                      Latest posts and engagement data
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <label htmlFor="limit-select" className="text-sm font-medium text-muted-foreground">
                      Show:
                    </label>
                    <select
                      id="limit-select"
                      value={limit}
                      onChange={(e) => handleLimitChange(Number(e.target.value))}
                      className="h-9 px-3 py-1 text-sm bg-background border border-input rounded-md focus:outline-none focus:ring-2 focus:ring-ring focus:border-transparent"
                    >
                      <option value={10}>10</option>
                      <option value={20}>20</option>
                      <option value={30}>30</option>
                      <option value={50}>50</option>
                      <option value={100}>100</option>
                    </select>
                    <span className="text-sm text-muted-foreground">entries</span>
                  </div>
                </div>
              </div>
            )}
            <div className="p-6">
              <DataTable
                data={data}
                isLoading={isLoading}
                onPageChange={handlePageChange}
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
