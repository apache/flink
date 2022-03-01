import { EditorOptions } from 'ng-zorro-antd/code-editor/typings';

export const flinkEditorOptions: EditorOptions = {
  scrollBeyondLastLine: false,
  glyphMargin: true,
  language: 'apex',
  wordWrap: 'on',
  readOnly: true,
  minimap: {
    enabled: false
  }
};
