import type { ReactNode } from "react";

import { cn } from "./helpers.js";

interface LoadingProps {
  className?: string;
}

/**
 * Loading component.
 * div style can be overridden by className prop.
 *
 * @param {Object} props
 * @param {string | undefined} props.className - additional class names to apply to the div
 * @returns {ReactNode}
 */
export default function Loading({ className }: LoadingProps): ReactNode {
  return <div className={cn("loading", className)}></div>;
}
