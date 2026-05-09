// Tests for the v0.6.0 VirtualizedBagList — Sub-feature C.7.
//
// Verifies the activation-threshold logic: small lists render flat
// (every bag in the DOM), large lists virtualize (only a windowed
// subset in the DOM at any time).

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import VirtualizedBagList from './VirtualizedBagList'
import type { Bag } from '../api'


function makeBag(id: number, overrides: Partial<Bag> = {}): Bag {
  return {
    id,
    path: `/data/bag_${id}.mcap`,
    duration_sec: 10,
    size_bytes: 1024 * 1024 * 50,
    message_count: 1000,
    health_score: 95,
    topics: [
      { name: '/imu/data', message_type: 'sensor_msgs/Imu', message_count: 500 },
    ],
    tags: [],
    ...overrides,
  }
}

function withRouter(node: React.ReactNode) {
  return <MemoryRouter>{node}</MemoryRouter>
}


describe('VirtualizedBagList', () => {
  it('renders an empty list cleanly', () => {
    const { container } = render(withRouter(<VirtualizedBagList bags={[]} />))
    // No bag links rendered
    expect(container.querySelectorAll('a').length).toBe(0)
  })

  it('renders all bags inline below the virtualization threshold', () => {
    const bags = Array.from({ length: 50 }, (_, i) => makeBag(i))
    render(withRouter(<VirtualizedBagList bags={bags} />))
    // All 50 should be in the DOM (flat path, no virtualization)
    const links = screen.getAllByRole('link')
    expect(links.length).toBe(50)
  })

  it('virtualizes large lists', () => {
    const bags = Array.from({ length: 500 }, (_, i) => makeBag(i))
    render(withRouter(<VirtualizedBagList bags={bags} height={300} />))
    // Virtualized: not all 500 should be in the DOM
    const links = screen.queryAllByRole('link')
    expect(links.length).toBeLessThan(500)
    expect(links.length).toBeGreaterThan(0)
  })

  it('renders bag basename in the row', () => {
    const bags = [makeBag(7, { path: '/some/where/named_bag.mcap' })]
    render(withRouter(<VirtualizedBagList bags={bags} />))
    expect(screen.getByText('named_bag.mcap')).toBeInTheDocument()
  })

  it('renders bag stats (duration, topics, messages)', () => {
    const bags = [makeBag(1, {
      duration_sec: 12.5,
      message_count: 7500,
    })]
    render(withRouter(<VirtualizedBagList bags={bags} />))
    expect(screen.getByText('12.5s')).toBeInTheDocument()
    expect(screen.getByText('1 topics')).toBeInTheDocument()
    expect(screen.getByText('7,500 msgs')).toBeInTheDocument()
  })

  it('formats sizes in MB', () => {
    const bags = [makeBag(1, { size_bytes: 50 * 1024 * 1024 })]
    render(withRouter(<VirtualizedBagList bags={bags} />))
    expect(screen.getByText(/50\.0 MB/)).toBeInTheDocument()
  })
})
