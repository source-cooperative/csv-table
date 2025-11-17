import type { ReactNode } from 'react'

import { cn } from './helpers.js'

interface LayoutProps {
  children: ReactNode
  className?: string
  error?: Error
}

/**
 * Layout for shared UI.
 * Content div style can be overridden by className prop.
 * @param props - layout properties
 * @param props.children - content to display inside the layout
 * @param props.className - additional class names to apply to the content container
 * @returns Layout component
 */
export default function Layout({
  children,
  className,
  // error, // TODO(SL): implement error bar
}: LayoutProps): ReactNode {
  return (
    <>
      <div className="content-container">
        <div className={cn('content', className)}>{children}</div>
        {/* <ErrorBar error={error} /> */}
      </div>
    </>
  )
}
