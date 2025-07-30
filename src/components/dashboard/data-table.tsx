'use client';

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  Pagination,
  PaginationContent,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
} from '@/components/ui/pagination';
import { Skeleton } from '@/components/ui/skeleton';
import { Loader } from '@/components/ui/loader';
import { ProcessedData, PaginatedResponse } from '@/types';

interface DataTableProps {
  data: PaginatedResponse<ProcessedData> | null;
  isLoading: boolean;
  onPageChange: (page: number) => void;
}

export function DataTable({ data, isLoading, onPageChange }: DataTableProps) {
  if (isLoading) {
    return (
      <div className="w-full flex justify-center items-center py-12">
        <Loader text="Loading data..." size="lg" />
      </div>
    );
  }

  if (
    !data ||
    !data.data ||
    !Array.isArray(data.data) ||
    data.data.length === 0
  ) {
    return (
      <div className="text-center py-8">
        <p className="text-gray-500">No data available</p>
      </div>
    );
  }

  const generatePageNumbers = () => {
    const pages = [];
    const totalPages = data.totalPages || 1;
    const currentPage = data.page || 1;

    for (
      let i = Math.max(1, currentPage - 2);
      i <= Math.min(totalPages, currentPage + 2);
      i++
    ) {
      pages.push(i);
    }

    return pages;
  };

  console.log('data', data);

  return (
    <div className="space-y-4">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Name</TableHead>
            <TableHead>Platform</TableHead>
            <TableHead>Brand</TableHead>
            <TableHead>Likes</TableHead>
            <TableHead>Comments</TableHead>
            <TableHead>Shares</TableHead>
            <TableHead>Reach</TableHead>
            <TableHead>Sales</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.data.map((item, index) => (
            <TableRow key={index}>
              <TableCell>{item.name}</TableCell>
              <TableCell>{item.platform}</TableCell>
              <TableCell>{item.brand}</TableCell>
              <TableCell>{item.likes.toLocaleString()}</TableCell>
              <TableCell>{item.comments.toLocaleString()}</TableCell>
              <TableCell>{item.shares.toLocaleString()}</TableCell>
              <TableCell>{item.reach.toLocaleString()}</TableCell>
              <TableCell>
                ${item.total_sales_attributed.toLocaleString()}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      {data.totalPages && data.totalPages > 1 && (
        <Pagination>
          <PaginationContent>
            <PaginationItem>
              <PaginationPrevious
                onClick={() =>
                  (data.page || 1) > 1 && onPageChange((data.page || 1) - 1)
                }
                className={
                  (data.page || 1) <= 1
                    ? 'pointer-events-none opacity-50'
                    : 'cursor-pointer'
                }
              />
            </PaginationItem>

            {generatePageNumbers().map((pageNum) => (
              <PaginationItem key={pageNum}>
                <PaginationLink
                  onClick={() => onPageChange(pageNum)}
                  isActive={pageNum === (data.page || 1)}
                  className="cursor-pointer"
                >
                  {pageNum}
                </PaginationLink>
              </PaginationItem>
            ))}

            <PaginationItem>
              <PaginationNext
                onClick={() =>
                  (data.page || 1) < (data.totalPages || 1) &&
                  onPageChange((data.page || 1) + 1)
                }
                className={
                  (data.page || 1) >= (data.totalPages || 1)
                    ? 'pointer-events-none opacity-50'
                    : 'cursor-pointer'
                }
              />
            </PaginationItem>
          </PaginationContent>
        </Pagination>
      )}
    </div>
  );
}
