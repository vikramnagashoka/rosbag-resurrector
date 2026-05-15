// Tests for v0.6 UI primitives (Button / Card / Badge).
// Lightweight — verifies rendering, prop forwarding, click handling.
//
// These run under jsdom; CSS-module class strings are hashed so we
// match by data attributes / role / text rather than class names.

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Button, Card, CardHeader, CardBody, CardFooter, Badge } from './index'


describe('Button', () => {
  it('renders children', () => {
    render(<Button>Click me</Button>)
    expect(screen.getByRole('button', { name: 'Click me' })).toBeInTheDocument()
  })

  it('fires onClick', () => {
    const onClick = vi.fn()
    render(<Button onClick={onClick}>X</Button>)
    fireEvent.click(screen.getByRole('button'))
    expect(onClick).toHaveBeenCalledTimes(1)
  })

  it('does not fire onClick when disabled', () => {
    const onClick = vi.fn()
    render(<Button onClick={onClick} disabled>X</Button>)
    fireEvent.click(screen.getByRole('button'))
    expect(onClick).not.toHaveBeenCalled()
  })

  it('forwards arbitrary HTML attributes', () => {
    render(<Button data-testid="my-btn" type="submit">Send</Button>)
    const btn = screen.getByTestId('my-btn')
    expect(btn).toHaveAttribute('type', 'submit')
  })

  it('supports all variants without crashing', () => {
    const variants = ['primary', 'secondary', 'ghost', 'danger'] as const
    for (const v of variants) {
      const { unmount } = render(<Button variant={v}>{v}</Button>)
      expect(screen.getByRole('button', { name: v })).toBeInTheDocument()
      unmount()
    }
  })

  it('supports all sizes without crashing', () => {
    const sizes = ['sm', 'md', 'lg'] as const
    for (const s of sizes) {
      const { unmount } = render(<Button size={s}>{s}</Button>)
      expect(screen.getByRole('button', { name: s })).toBeInTheDocument()
      unmount()
    }
  })

  it('forwards ref', () => {
    let captured: HTMLButtonElement | null = null
    render(
      <Button ref={(el) => { captured = el }}>X</Button>,
    )
    expect(captured).toBeInstanceOf(HTMLButtonElement)
  })
})


describe('Card', () => {
  it('renders bare card with children', () => {
    render(<Card><div>content</div></Card>)
    expect(screen.getByText('content')).toBeInTheDocument()
  })

  it('renders with header / body / footer slots', () => {
    render(
      <Card>
        <CardHeader>Title</CardHeader>
        <CardBody>Body text</CardBody>
        <CardFooter>Footer</CardFooter>
      </Card>,
    )
    expect(screen.getByText('Title')).toBeInTheDocument()
    expect(screen.getByText('Body text')).toBeInTheDocument()
    expect(screen.getByText('Footer')).toBeInTheDocument()
  })

  it('hoverable prop adds an extra class', () => {
    const { container } = render(
      <Card hoverable><div>x</div></Card>,
    )
    // CSS-modules hash class names; check classList has more entries
    // when hoverable is set
    const div = container.firstChild as HTMLElement
    expect(div.className.split(' ').length).toBeGreaterThanOrEqual(2)
  })

  it('forwards arbitrary props to root div', () => {
    render(<Card data-testid="c"><div>x</div></Card>)
    expect(screen.getByTestId('c')).toBeInTheDocument()
  })
})


describe('Badge', () => {
  it('renders text', () => {
    render(<Badge>healthy</Badge>)
    expect(screen.getByText('healthy')).toBeInTheDocument()
  })

  it('supports all variants', () => {
    const variants = [
      'neutral', 'success', 'warning', 'danger', 'info', 'accent',
    ] as const
    for (const v of variants) {
      const { unmount } = render(<Badge variant={v}>{v}</Badge>)
      expect(screen.getByText(v)).toBeInTheDocument()
      unmount()
    }
  })

  it('forwards arbitrary props', () => {
    render(<Badge data-testid="b">x</Badge>)
    expect(screen.getByTestId('b')).toBeInTheDocument()
  })
})
