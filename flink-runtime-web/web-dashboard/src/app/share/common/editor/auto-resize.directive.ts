import { Directive, ElementRef, OnDestroy, OnInit, Renderer2 } from '@angular/core';
import { animationFrameScheduler, interval, Observable, Subject } from 'rxjs';
import { debounceTime, distinctUntilChanged, takeUntil } from 'rxjs/operators';

import { NzCodeEditorComponent } from 'ng-zorro-antd/code-editor';

@Directive({
  selector: 'nz-code-editor[flinkAutoResize]'
})
export class AutoResizeDirective implements OnDestroy, OnInit {
  private destroy$ = new Subject();
  hiddenMinimap = false;

  constructor(
    private elementRef: ElementRef<HTMLElement>,
    private nzCodeEditorComponent: NzCodeEditorComponent,
    private renderer: Renderer2
  ) {}

  public ngOnInit(): void {
    this.createResizeObserver()
      .pipe(
        distinctUntilChanged((prev, curr) => {
          const { width: prevWidth, height: prevHeight } = prev;
          const { width: currWidth, height: currHeight } = curr;
          return prevWidth === currWidth && prevHeight === currHeight;
        }),
        debounceTime(50, animationFrameScheduler),
        takeUntil(this.destroy$)
      )
      .subscribe(curr => {
        const curWidth = curr.width;
        this.hiddenMinimap = curWidth <= 65;
        this.setHostClass();
        this.nzCodeEditorComponent.layout();
      });
  }

  private setHostClass(): void {
    if (this.hiddenMinimap) {
      this.renderer.addClass(this.elementRef.nativeElement, 'hidden-minimap');
    } else {
      this.renderer.removeClass(this.elementRef.nativeElement, 'hidden-minimap');
    }
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private createResizeObserver(): Observable<DOMRectReadOnly> {
    const stream = new Subject<DOMRectReadOnly>();
    const ResizeObserver = window.ResizeObserver;
    if (ResizeObserver) {
      // https://developer.mozilla.org/en-US/docs/Web/API/ResizeObserver
      // ResizeObserver is supported except IE
      const resizeObserver = new ResizeObserver(entries => {
        for (const entry of entries) {
          if (entry.target === this.elementRef.nativeElement) {
            stream.next(entry.contentRect);
          }
        }
      });
      resizeObserver.observe(this.elementRef.nativeElement);
      this.destroy$.subscribe(() => resizeObserver.disconnect());
    } else if (typeof this.elementRef.nativeElement?.getBoundingClientRect === 'function') {
      interval(500)
        .pipe(takeUntil(this.destroy$))
        .subscribe(() => {
          stream.next(this.elementRef.nativeElement.getBoundingClientRect());
        });
    }
    return stream.asObservable();
  }
}
