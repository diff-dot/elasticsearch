import { DocumentMetadata } from './DocumentMetadata';

export interface RoutedDocumentMetadata extends DocumentMetadata {
  routing: string;
}
