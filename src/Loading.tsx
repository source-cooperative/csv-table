import type { ReactNode } from 'react'

import { cn } from './helpers.js'

interface LoadingProps {
  className?: string
}

/**
 * Loading component.
 * div style can be overridden by className prop.
 * @param props - loading properties
 * @param props.className - additional class names to apply to the div
 * @returns Loading component
 */
export default function Loading({ className }: LoadingProps): ReactNode {
  return <div className={cn('loading', className)}></div>
}
