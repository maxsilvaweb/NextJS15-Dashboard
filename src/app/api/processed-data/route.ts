import { NextRequest, NextResponse } from 'next/server';
import { api } from '@/lib/api';

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const page = parseInt(searchParams.get('page') || '1');
    const limit = parseInt(searchParams.get('limit') || '10');
    const user_id = searchParams.get('user_id') || undefined;
    const platform = searchParams.get('platform') || undefined;
    const program_id = searchParams.get('program_id') || undefined;

    const data = await api.getProcessedData({
      page,
      limit,
      user_id,
      platform,
      program_id,
    });

    return NextResponse.json(data);
  } catch (error) {
    console.error('API Error:', error);
    return NextResponse.json(
      { error: 'Failed to fetch data' },
      { status: 500 }
    );
  }
}