'use client';

import { useEffect, useRef } from 'react';
import * as d3 from 'd3';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { ProcessedData } from '@/types';

interface ChartsProps {
  data: ProcessedData[];
}

export function Charts({ data }: ChartsProps) {
  const barChartRef = useRef<SVGSVGElement>(null);
  const pieChartRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!data.length) return;

    // Platform engagement bar chart
    const platformData = d3.rollup(
      data,
      v => d3.sum(v, d => d.likes + d.comments + d.shares),
      d => d.platform
    );

    const barData = Array.from(platformData, ([platform, engagement]) => ({
      platform,
      engagement,
    }));

    // Clear previous chart
    d3.select(barChartRef.current).selectAll('*').remove();

    const margin = { top: 20, right: 30, bottom: 40, left: 40 };
    const width = 400 - margin.left - margin.right;
    const height = 200 - margin.bottom - margin.top;

    const svg = d3.select(barChartRef.current)
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom);

    const g = svg.append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    const x = d3.scaleBand()
      .rangeRound([0, width])
      .padding(0.1)
      .domain(barData.map(d => d.platform));

    const y = d3.scaleLinear()
      .rangeRound([height, 0])
      .domain([0, d3.max(barData, d => d.engagement) || 0]);

    g.append('g')
      .attr('transform', `translate(0,${height})`)
      .call(d3.axisBottom(x));

    g.append('g')
      .call(d3.axisLeft(y));

    g.selectAll('.bar')
      .data(barData)
      .enter().append('rect')
      .attr('class', 'bar')
      .attr('x', d => x(d.platform) || 0)
      .attr('y', d => y(d.engagement))
      .attr('width', x.bandwidth())
      .attr('height', d => height - y(d.engagement))
      .attr('fill', '#3b82f6');

  }, [data]);

  useEffect(() => {
    if (!data.length) return;

    // Sales attribution pie chart
    const salesData = d3.rollup(
      data,
      v => d3.sum(v, d => d.total_sales_attributed),
      d => d.brand
    );

    const pieData = Array.from(salesData, ([brand, sales]) => ({
      brand,
      sales,
    })).filter(d => d.sales > 0);

    // Clear previous chart
    d3.select(pieChartRef.current).selectAll('*').remove();

    const width = 300;
    const height = 200;
    const radius = Math.min(width, height) / 2;

    const svg = d3.select(pieChartRef.current)
      .attr('width', width)
      .attr('height', height);

    const g = svg.append('g')
      .attr('transform', `translate(${width / 2},${height / 2})`);

    const color = d3.scaleOrdinal(d3.schemeCategory10);

    const pie = d3.pie<any>()
      .value(d => d.sales);

    const arc = d3.arc<any>()
      .innerRadius(0)
      .outerRadius(radius - 10);

    const arcs = g.selectAll('.arc')
      .data(pie(pieData))
      .enter().append('g')
      .attr('class', 'arc');

    arcs.append('path')
      .attr('d', arc)
      .attr('fill', (d, i) => color(i.toString()));

    arcs.append('text')
      .attr('transform', d => `translate(${arc.centroid(d)})`)
      .attr('dy', '.35em')
      .style('text-anchor', 'middle')
      .style('font-size', '12px')
      .text(d => d.data.brand);

  }, [data]);

  return (
    <div className="grid gap-4 md:grid-cols-2">
      <Card>
        <CardHeader>
          <CardTitle>Platform Engagement</CardTitle>
        </CardHeader>
        <CardContent>
          <svg ref={barChartRef}></svg>
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader>
          <CardTitle>Sales by Brand</CardTitle>
        </CardHeader>
        <CardContent>
          <svg ref={pieChartRef}></svg>
        </CardContent>
      </Card>
    </div>
  );
}