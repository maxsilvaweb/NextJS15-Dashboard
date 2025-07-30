import { cn } from "@/lib/utils";

interface LoaderProps {
  className?: string;
  text?: string;
  size?: "sm" | "md" | "lg";
}

function Loader({ className, text = "Loading...", size = "md" }: LoaderProps) {
  const sizeClasses = {
    sm: "w-4 h-4",
    md: "w-6 h-6",
    lg: "w-8 h-8"
  };

  return (
    <div className={cn("flex items-center justify-center gap-2", className)}>
      <div
        className={cn(
          "animate-spin rounded-full border-2 border-gray-300 border-t-blue-600",
          sizeClasses[size]
        )}
      />
      <span className="text-sm text-muted-foreground">{text}</span>
    </div>
  );
}

export { Loader };